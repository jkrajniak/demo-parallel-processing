package logging

import (
	"github.com/sirupsen/logrus"
	"os"
)

func ConfigureLogger(levelName string) {
	logrus.SetFormatter(&logrus.JSONFormatter{})
	logLevel := logrus.ErrorLevel
	if levelName != "" {
		var err error
		logLevel, err = logrus.ParseLevel(levelName)
		if err != nil {
			logrus.Warningf("unable to parse log level %s - switch back to ErrorLevel (error: %s)", logLevel, err.Error())
		}
	}
	logrus.SetLevel(logLevel)
}

func ConfigureLoggerFromEnv(envName string) {
	lvlName := os.Getenv(envName)
	ConfigureLogger(lvlName)
}
