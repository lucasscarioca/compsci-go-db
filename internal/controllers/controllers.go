package controllers

import (
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"
	"github.com/lucasscarioca/custom-db/internal/db"
)

func Home(c echo.Context) error {
	ctx := echo.Map{
		"Stats": db.DB.GetHashIndexStats(),
	}

	return c.Render(http.StatusOK, "home", ctx)
}

func Search(c echo.Context) error {
	res, err := db.DB.Find(c.FormValue("search"))
	if err != nil {
		return c.Render(http.StatusOK, "not_found", nil)
	}
	ctx := echo.Map{
		"Result": res,
	}

	return c.Render(http.StatusOK, "search_response", ctx)
}

func TableScan(c echo.Context) error {
	n, err := strconv.Atoi(c.FormValue("nRegisters"))
	if err != nil {
		c.Render(http.StatusOK, "not_found", nil)
	}
	res := db.DB.TableScan(n)

	ctx := echo.Map{
		"Result": res.Data,
	}

	return c.Render(http.StatusOK, "scan_response", ctx)
}
