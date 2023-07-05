package main

import (
	"dbload/kafka/config"
	"dbload/kafka/consumer/internal"
	"dbload/kafka/logger"
	log "github.com/sirupsen/logrus"
)

/* review:
отсутвует обраюока ошибок
*/

func main() {
	conf := config.ParseConfig()
	readerLogger := log.New()
	logger.SetupReaderLogging(conf, readerLogger)
	internal.StartConsuming(&conf, readerLogger)
}
