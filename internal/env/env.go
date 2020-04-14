package env

import (
	"fmt"
	"os"
)

func LoadEnvVariableOrPanic(envVarName string) string {
	varValue, isVarSet := os.LookupEnv(envVarName)
	if !isVarSet {
		panic(fmt.Sprintf("variable %s is not set", envVarName))
	}
	return varValue
}
