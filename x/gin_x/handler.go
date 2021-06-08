package gin_x

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"
	"github.com/shyyawn/go-to/x/logging"
)

// SendResponse setting gin.JSON
func SendResponse(ctx *gin.Context, httpCode int, data interface{}) {
	if httpCode >= 400 {
		panic("Cannot pass an error http code in Response.")
	}
	ctx.JSON(httpCode, data)
}

// SendResponseError setting gin.JSON
func SendResponseError(ctx *gin.Context, httpCode int, data interface{}) {
	if httpCode < 400 {
		panic("Cannot pass a non error http code in Response.")
	}

	ctx.JSON(httpCode, data)
}

// SendResponseValidationError setting gin.JSON
func SendResponseValidationError(ctx *gin.Context, httpCode int, err error) {
	if httpCode < 400 {
		panic("Cannot pass a non error http code in Response.")
	}
	for _, err := range err.(validator.ValidationErrors) {

		fmt.Println("Namespace", err.Namespace())
		fmt.Println("Field", err.Field())
		fmt.Println("Struct Namespace", err.StructNamespace())
		fmt.Println("Struct Field", err.StructField())
		fmt.Println("Tag", err.Tag())
		fmt.Println("Actual Tag", err.ActualTag())
		fmt.Println("Kind", err.Kind())
		fmt.Println("Type", err.Type())
		fmt.Println("Value", err.Value())
		fmt.Println("Params: ", err.Param())
		fmt.Println()
	}
	ctx.JSON(httpCode, err.Error())
}

func SendResponseString(ctx *gin.Context, httpCode int, format string, data *string) {
	if httpCode >= 400 {
		panic("Cannot pass an error http code in Response String.")
	}
	//logging.Info(data)
	ctx.String(httpCode, format, *data)
}

func SendResponseErrorString(ctx *gin.Context, httpCode int, data interface{}, err error) {
	if httpCode < 400 {
		panic("Cannot pass a non error http code in Response Error.")
	}
	logging.Error(data, err)
	ctx.String(httpCode, "%s", data)
}
