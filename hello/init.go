package hello

import (
	"database/sql"
	"encoding/json"
	"expvar"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
	_ "github.com/lib/pq"
	"github.com/nsqio/go-nsq"
	"github.com/tokopedia/sqlt"
	logging "gopkg.in/tokopedia/logging.v1"
)

type ServerConfig struct {
	Name string
}
type DatabaseConfig struct {
	Type       string
	Connection string
}

type Config struct {
	Server   ServerConfig
	Database DatabaseConfig
	NSQ      NSQConfig
	Redis    RedisConfig
}

type RedisConfig struct {
	Protocol string
	Host     string
}

type NSQConfig struct {
	NSQD     string
	Lookupds string
}

type HelloWorldModule struct {
	cfg       *Config
	NSQProd   *nsq.Producer
	NSQCon    *nsq.Consumer
	DB        *sqlt.DB
	Redis     *redis.Pool
	something string
	stats     *expvar.Int
}

/* Inisialisasi Module Hello World */
func NewHelloWorldModule() *HelloWorldModule {

	var cfg Config

	// Read Config from hello.development.ini
	ok := logging.ReadModuleConfig(&cfg, "config", "hello") || logging.ReadModuleConfig(&cfg, "files/etc/gosample", "hello")
	if !ok {
		// when the app is run with -e switch, this message will automatically be redirected to the log file specified
		log.Fatalln("failed to read config")
	}

	//configure database connection
	masterDB := cfg.Database.Connection
	slaveDB := cfg.Database.Connection
	dbConnection := fmt.Sprintf("%s;%s", masterDB, slaveDB)
	db, err := sqlt.Open(cfg.Database.Type, dbConnection) // open database connection
	if err != nil {
		log.Fatalln("Failed to connect database. Error: ", err.Error())
	}

	redis := &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			conn, err := redis.Dial(cfg.Redis.Protocol, cfg.Redis.Host)
			if err != nil {
				return nil, err
			}
			return conn, err
		},
	}
	consumer, err := nsq.NewConsumer("random-topic-2", "ch", nsq.NewConfig())
	consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		fmt.Println(string(message.Body))
		data := map[string]string{}
		json.Unmarshal(message.Body, &data)
		output, err := redis.Get().Do("SET", "WelcomeMessage", data["message"]+" "+data["name"])
		if err != nil {
			log.Println("Unable to save message to redis ")
			message.Requeue(10 * time.Minute)
			return err
		}
		redis.Get().Do("EXPIRE", "WelcomeMessage", 10*time.Minute)
		log.Println("Success to receive and save message to redis ")
		fmt.Println(output)
		message.Finish()
		return nil

	}))
	consumer.ConnectToNSQLookupd(cfg.NSQ.Lookupds)
	producer, err := nsq.NewProducer(cfg.NSQ.NSQD, nsq.NewConfig())
	if err != nil {
		fmt.Println("Unable to create Producer because :", err)
	}

	return &HelloWorldModule{
		cfg:       &cfg,
		DB:        db,
		NSQCon:    consumer,
		NSQProd:   producer,
		Redis:     redis,
		something: "John Doe",
		stats:     expvar.NewInt("rpsStats"),
	}
}

func (hlm *HelloWorldModule) SayHelloWorld(w http.ResponseWriter, r *http.Request) {
	hlm.stats.Add(1)
	w.Write([]byte("Hello " + hlm.something))
}

type ZakiTest struct {
	ID       int64
	FullName string `db:"name"`
}

type User struct {
	ID         int64          `db:"user_id"`
	UserName   sql.NullString `db:"full_name"`
	Msisdn     sql.NullString `db:"msisdn"`
	Email      string         `db:"user_email"`
	BirthDate  sql.NullString `db:"birth_date"`
	CreateTime sql.NullString `db:"create_time"`
	UpdateTime sql.NullString `db:"update_time"`
	Age        sql.NullInt64  `db:"age"`
}

type PageData struct {
	PageTitle       string
	WelcomeMessage  interface{}
	WebVisitorCount interface{}
	Users           []User
}

func (hlm *HelloWorldModule) GetSingleDataFromDatabase(w http.ResponseWriter, r *http.Request) {
	hlm.stats.Add(1)

	test := ZakiTest{}
	query := "SELECT id, name FROM zaki_test LIMIT 1"
	err := hlm.DB.Get(&test, query)
	if err != nil {
		log.Println("Error Query Database. Error: ", err.Error())
	}

	result := fmt.Sprintf("Hello User ID %d with Name %s", test.ID, test.FullName)

	w.Write([]byte(result))
}

func (hlm *HelloWorldModule) SendMessage(name, message string) {
	data := map[string]string{
		"name":    name,
		"message": message,
	}
	nsqMessage, _ := json.Marshal(data)
	err := hlm.NSQProd.Publish("random-topic-2", nsqMessage)

	if err != nil {
		log.Println("Failed to publish NSQ message. Error: ", err)
	} else {
		log.Println("Success to Publis NSQ Message")
	}
}

func (hlm *HelloWorldModule) GetMultiDataFromDatabase(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	hlm.stats.Add(1)
	pool := hlm.Redis.Get()
	webVisitorCount, err := pool.Do("INCR", "webVisitorCount")
	if err != nil {
		log.Println(err)
	}
	welcomeMessage, err := redis.String(pool.Do("GET", "WelcomeMessage"))
	if err != nil {
		log.Println(err)
	}
	users := []User{}
	query := `SELECT 
              user_id, 
              full_name, 
              msisdn, 
              user_email, 
              to_char(birth_date, 'YYYY/MM/DD') AS birth_date, 
              to_char(create_time, 'YYYY/MM/DD') AS create_time, 
              to_char(update_time, 'YYYY/MM/DD') AS update_time, 
              extract(YEAR FROM age(birth_date)) age 
              FROM ws_user
			  ORDER BY full_name 
              LIMIT 10
			  `
	err = hlm.DB.Select(&users, query)
	if err != nil {
		log.Println("Error Query Database. Error: ", err.Error())
	}
	tmpl, err := template.ParseFiles("files/html/layout.html")

	if err != nil {
		log.Println("Unable to parse html file ", err)
	}
	fmt.Println(welcomeMessage)
	data := PageData{
		PageTitle:       "My First Task 1",
		WelcomeMessage:  welcomeMessage,
		WebVisitorCount: webVisitorCount,
		Users:           users,
	}
	tmpl.Execute(w, data)

}

func (hlm *HelloWorldModule) ChangeWelcomeMessage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	name := r.FormValue("name")
	message := r.FormValue("message")

	hlm.SendMessage(name, message)
}

func (hlm *HelloWorldModule) SearchDataFromDatabase(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	redisPool := hlm.Redis.Get()
	hlm.stats.Add(1)
	name := r.FormValue("name")

	users := []User{}
	query := `SELECT 
              user_id, 
              full_name, 
              msisdn, 
              user_email, 
              to_char(birth_date, 'YYYY/MM/DD') AS birth_date, 
              to_char(create_time, 'YYYY/MM/DD') AS create_time, 
              to_char(update_time, 'YYYY/MM/DD') AS update_time,
              extract(YEAR FROM age(birth_date)) age 
              FROM ws_user
              WHERE lower(full_name) LIKE $1 
			  ORDER BY full_name
              LIMIT 10 
			   `

	err := hlm.DB.Select(&users, query, "%"+strings.ToLower(name)+"%")
	if err != nil {
		log.Println("Error Query Database. Error: ", err.Error())
	}

	/*data := PageData{
		PageTitle: "My First Task",
		Users:     users,
	}
	for _, value := range data.Users {
		fmt.Println(value.UserName.String)
	}*/
	out, err := json.Marshal(users)

	redisPool.Do("")
	elapse := time.Now().Sub(start)
	fmt.Println(elapse)
	w.Write(out)
}

func (hlm *HelloWorldModule) GetTitle(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	pool := hlm.Redis.Get()
	welcomeMessage, err := redis.String(pool.Do("GET", "WelcomeMessage"))
	if err != nil {
		welcomeMessage = "Welcome Aboard"
	}

	if len(welcomeMessage) == 0 {
		welcomeMessage = "Welcome Aboard"
	}
	w.Write([]byte(welcomeMessage))
}
