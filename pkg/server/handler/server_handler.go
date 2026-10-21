package server_handler

import (
	"database/sql"
	"expvar"
	"net/http"
	"runtime"

	"github.com/gin-gonic/gin"
)

type ServerHandler interface {
	ServerOk(ctx *gin.Context)
	Metrics(ctx *gin.Context)
}

type serverHandler struct {
	authDB *sql.DB
}

// ServerOk agora checa de verdade: ping no pool authDB e devolve contagens de
// runtime/go. O handler antigo respondia {"status":"ok"} estatico — o Powzz
// usa este endpoint como gate do cron de reconexao, e "ok" enquanto o authDB
// estava saturado fazia o cron continuar martelando um Evolution morto.
func (s *serverHandler) ServerOk(ctx *gin.Context) {
	status := http.StatusOK
	checks := gin.H{}

	if s.authDB != nil {
		if err := s.authDB.Ping(); err != nil {
			status = http.StatusServiceUnavailable
			checks["authDB"] = "error: " + err.Error()
		} else {
			st := s.authDB.Stats()
			checks["authDB"] = gin.H{
				"state":     "ok",
				"open":      st.OpenConnections,
				"inUse":     st.InUse,
				"idle":      st.Idle,
				"waitCount": st.WaitCount,
			}
		}
	}

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	checks["goroutines"] = runtime.NumGoroutine()
	checks["memSysMB"] = ms.Sys / (1024 * 1024)

	ctx.JSON(status, gin.H{
		"status": map[bool]string{true: "ok", false: "degraded"}[status == http.StatusOK],
		"checks": checks,
	})
}

// Metrics serve o dump do expvar (contadores de reconexão, clients, etc).
func (s *serverHandler) Metrics(ctx *gin.Context) {
	expvar.Handler().ServeHTTP(ctx.Writer, ctx.Request)
}

func NewServerHandler(authDB *sql.DB) ServerHandler {
	return &serverHandler{
		authDB: authDB,
	}
}
