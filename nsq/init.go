package nsq

import (
	"log"
	"os"

	"github.com/nsqio/go-nsq"
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

type NSQModule struct {
	cfg *Config
	q   *nsq.Consumer
}

func NewNSQModule() *NSQModule {

	var cfg Config

	ok := logging.ReadModuleConfig(&cfg, "config", "hello") || logging.ReadModuleConfig(&cfg, "files/etc/gosample", "hello")
	if !ok {
		// when the app is run with -e switch, this message will automatically be redirected to the log file specified
		log.Fatalln("failed to read config")
	}

	// this message only shows up if app is run with -debug option, so its great for debugging
	logging.Debug.Println("nsq init called", cfg.Server.Name)

	// contohnya: caranya ciptakan nsq consumer
	nsqCfg := nsq.NewConfig()
	q := createNewConsumer(nsqCfg, "random-topic-2", "test", handler)
	q.SetLogger(log.New(os.Stderr, "nsq:", log.Ltime), nsq.LogLevelError)
	q.ConnectToNSQLookupd("devel-go.tkpd:4161")

	return &NSQModule{
		cfg: &cfg,
		q:   q,
	}

}

func handler(msg *nsq.Message) error {
	log.Println("got message :", string(msg.Body))
	msg.Finish()
	return nil
}

func createNewConsumer(nsqCfg *nsq.Config, topic string, channel string, handler nsq.HandlerFunc) *nsq.Consumer {
	q, err := nsq.NewConsumer(topic, channel, nsqCfg)
	if err != nil {
		log.Fatal("failed to create consumer for ", topic, channel, err)
	}
	q.AddHandler(handler)
	return q
}
