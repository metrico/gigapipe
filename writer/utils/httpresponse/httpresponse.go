package httpresponse

import (
	"github.com/gofiber/fiber/v3"
)

func CreateBadResponse(ctx fiber.Ctx, requestCode int, message string) error {

	return ctx.Status(requestCode).JSON(&fiber.Map{
		"success": false,
		"message": message,
	})
}

func CreateBadResponseWithCode(ctx fiber.Ctx, requestCode int, errorCode int, message string) error {

	return ctx.Status(requestCode).JSON(&fiber.Map{
		"success":   false,
		"errorcode": errorCode,
		"message":   message,
	})
}

func CreateSuccessResponse(ctx fiber.Ctx, requestCode int, message string) error {

	return ctx.Status(requestCode).JSON(&fiber.Map{
		"success": true,
		"message": message,
	})
}

func CreateBadResponseWithJson(ctx fiber.Ctx, requestCode int, message []byte) error {

	return ctx.Status(requestCode).JSON(&fiber.Map{
		"success": false,
		"message": message,
	})
}

func CreateSuccessResponseWithJson(ctx fiber.Ctx, requestCode int, message []byte) error {
	return ctx.Status(requestCode).JSON(&fiber.Map{
		"success": true,
		"message": message,
	})
}

func CreateSuccessResponseWTBody(ctx fiber.Ctx, requestCode int) error {
	return ctx.Status(requestCode).SendString("")
}
