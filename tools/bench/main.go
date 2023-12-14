/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>
*/
package main

import (
	"github.com/clyso/chorus/tools/bench/pkg/bench"
	"github.com/clyso/chorus/tools/bench/pkg/config"
	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		logrus.WithError(err).Fatal("Error loading .env file")
	}
	conf := config.Get()
	err = bench.Start(conf)
	if err != nil {
		logrus.WithError(err).Fatal("Benchmark exited with error")
	}
}
