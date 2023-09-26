package routes

import (
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/lucasscarioca/custom-db/internal/controllers"
	"github.com/lucasscarioca/custom-db/internal/http/middlewares"
	"github.com/lucasscarioca/custom-db/internal/views"
)

func Mount(e *echo.Echo) {
	e.HTTPErrorHandler = customHTTPErrorHandler
	e.Renderer = views.WithTemplateRegistry()

	e.GET("ping", pingHandler)

	// Pages Routes
	pages := e.Group("/", middlewares.CacheControl(0))
	pages.GET("", controllers.Home)
	pages.GET("404", pageNotFoundHandler)

	// Htmx Fragments Routes
	hx := e.Group("/hx", middlewares.CacheControl(0), middlewares.ValidateHxRequest)
	hx.POST("/search", controllers.Search)
	hx.POST("/tablescan", controllers.TableScan)
}

func customHTTPErrorHandler(err error, c echo.Context) {
	code := http.StatusInternalServerError
	if he, ok := err.(*echo.HTTPError); ok {
		code = he.Code
	}
	host := c.Request().Host
	URI := c.Request().RequestURI
	qs := c.QueryString()

	c.Logger().Error(err, fmt.Sprintf("\non: %s%s%s error code: %d", host, URI, qs, code))
	if code == 404 {
		c.Redirect(http.StatusTemporaryRedirect, "/404")
	}
	c.String(code, fmt.Sprintf("error code: %d", code))
}

func pingHandler(c echo.Context) error {
	return c.String(http.StatusOK, "pong")
}

// TODO: Test Redirect and handler
func pageNotFoundHandler(c echo.Context) error {
	return c.Render(http.StatusOK, "pageNotFound", map[string]any{})
}
