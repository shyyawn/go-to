package logging

import (
	"fmt"
	"github.com/go-playground/validator/v10"
	c "github.com/logrusorgru/aurora/v3"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"time"
)

type Level int

var (
	DefaultPrefix      = ""
	DefaultCallerDepth = 2
	DefaultLevel       = DEBUG

	loggerOut *log.Logger
	loggerErr *log.Logger

	levelFlags = []string{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}
)

const (
	DEBUG Level = iota
	INFO
	WARNING
	ERROR
	FATAL
)

func init() {
	loggerOut = log.New(os.Stdout, DefaultPrefix, 0)
	loggerErr = log.New(os.Stderr, DefaultPrefix, 0)
}

// Trace - only use this in case of emergency, and remove after use is done, highly avoid this
func Trace(v ...interface{}) {
	loggerOut.Println(getPrefix(INFO), v)
}

func Debug(v ...interface{}) {
	if DefaultLevel <= DEBUG {
		loggerOut.Println(getPrefix(DEBUG), v)
	}
}

func Info(v ...interface{}) {
	if DefaultLevel <= INFO {
		loggerOut.Println(getPrefix(INFO), v)
	}
}

func Warn(v ...interface{}) {
	if DefaultLevel <= WARNING {
		loggerErr.Println(getPrefix(WARNING), v, string(debug.Stack()))
	}
}

func Error(v ...interface{}) {
	if DefaultLevel <= ERROR {
		loggerErr.Println(getPrefix(ERROR), v, string(debug.Stack()))
	}
}

func Fatal(v ...interface{}) {
	if DefaultLevel <= FATAL {
		loggerErr.Println(getPrefix(FATAL), v)
		panic(v)
	}
}

// Tracef - only use this in case of emergency, and remove after use is done, highly avoid this
func Tracef(s string, v ...interface{}) {
	Trace(fmt.Sprintf(s, v...))
}

func Debugf(s string, v ...interface{}) {
	Debugf(fmt.Sprintf(s, v...))
}

func Infof(s string, v ...interface{}) {
	Info(fmt.Sprintf(s, v...))
}

func Warnf(s string, v ...interface{}) {
	Warn(fmt.Sprintf(s, v...))
}

func Errorf(s string, v ...interface{}) {
	Error(fmt.Sprintf(s, v...))
}

func Fatalf(s string, v ...interface{}) {
	Fatal(fmt.Sprintf(s, v...))
}

// InfoErrors prints all errors
func InfoErrors(err error) {
	for _, err := range err.(validator.ValidationErrors) {
		Info(fmt.Sprintf("Namespace %v\n"+"Field %v\n"+"Struct Namespace %v\n"+
			"Struct Field %v\n"+"Tag %v\n"+"Actual Tag %v\n"+
			"Kind %v\n"+"Type %v\n"+"Value %v\n"+"Param %v\n",
			err.Namespace(), err.Field(), err.StructNamespace(),
			err.StructField(), err.Tag(), err.ActualTag(),
			err.Kind(), err.Type(), err.Value(), err.Param()))
	}
}

func getPrefix(level Level) string {

	var logLevel, logFile, logDateTime string

	_, file, line, ok := runtime.Caller(DefaultCallerDepth)

	runtimeDir, err := filepath.Abs("./")

	if err != nil {
		log.Fatal(err)
	}

	logLevel = fmt.Sprintf("[%s]", levelFlags[level])

	if ok {
		logFile = fmt.Sprintf("[%s:%d]", strings.TrimPrefix(file, runtimeDir), line)
	} else {
		logFile = ""
	}

	switch level {
	case INFO:
		logLevel = c.BgBlue(logLevel).String()
		logFile = c.Blue(logFile).String()
		break
	case DEBUG:
		logLevel = c.BgGreen(logLevel).String()
		logFile = c.Green(logFile).String()
		break
	case FATAL:
	case ERROR:
		logLevel = c.BgRed(logLevel).String()
		logFile = c.Red(logFile).String()
		break
	case WARNING:
		logLevel = c.BgYellow(logLevel).String()
		logFile = c.Yellow(logFile).String()
		break
	}

	logDateTime = fmt.Sprintf("[%s]", time.Now().Format(time.RFC3339))

	return logLevel + logFile + logDateTime
}
