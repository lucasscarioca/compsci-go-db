package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/lucasscarioca/custom-db/internal/db"
	"github.com/lucasscarioca/custom-db/internal/http/middlewares"
	"github.com/lucasscarioca/custom-db/internal/http/routes"
)

func main() {
	err := db.Init()
	if err != nil {
		panic("Failed to instantiate database")
	}

	e := echo.New()

	middlewares.Mount(e)
	routes.Mount(e)

	fmt.Println("🚀 Starting server on port: http://localhost:3000")
	go func() {
		if err := e.Start(":3000"); err != nil && err != http.ErrServerClosed {
			e.Logger.Fatal("shutting down the server")
		}
	}()
	// Wait for interrupt signal to gracefully shutdown the server
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := e.Shutdown(ctx); err != nil {
		e.Logger.Fatal(err)
	}
}
