package main

import (
	"github.com/colinmarc/hdfs"
	log "github.com/sirupsen/logrus"
)

func main() {
	client, err := hdfs.New("localhost:9870")
	if err != nil {
		log.Println(err)
	}
	file, err := client.ReadFile("README.txt")
	if err != nil {
		log.Println(err)
	}
	log.Println(file)
}
