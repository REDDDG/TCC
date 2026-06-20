package middleware

import "github.com/gin-gonic/gin"

func LoggerSkip(skipPaths ...string) gin.HandlerFunc {
	config := gin.LoggerConfig{
		SkipPaths: skipPaths,
	}
	return gin.LoggerWithConfig(config)
}
