// Package metrics expoe contadores e gauges do processo via expvar, sob o
// prefixo "evolution". Sem dependencia externa: basta `expvar` + net/http.
//
// Motivacao: o bug de pool leak do 0.7.2 so era visivel via pg_stat_activity
// externo. Com estes sinais, um reconexao em espiral, um webhooks storm ou um
// authDB saturado aparecem no proprio /metrics antes de virar incidente.
package metrics

import (
	"expvar"
	"sync/atomic"
)

var (
	// Reconnects conta tentativas de ReconnectClient por resultado.
	Reconnects = expvar.NewMap("evolution_reconnects_total") // "success"|"error"|"backoff"|"kill_timeout"

	// DisconnectedEvents conta eventos Disconnected/KeepAliveTimeout recebidos
	// do whatsmeow — o sinal de flap de socket.
	DisconnectedEvents = expvar.NewInt("evolution_disconnected_events_total")

	// StreamErrors conta ocorrências de stream:error (a assinatura do bug #185).
	StreamErrors = expvar.NewInt("evolution_stream_errors_total")

	// WebhookFailures conta envios de webhook esgotados após retries.
	WebhookFailures = expvar.NewInt("evolution_webhook_failures_total")

	// ClientsRegistered e o número de clients no registry (Atualiza via gauges
	// do registry; aqui fica o último valor reportado).
	clientsRegistered atomic.Int64
	runtimesActive    atomic.Int64
)

func init() {
	expvar.Publish("evolution_clients_registered", expvar.Func(func() interface{} {
		return clientsRegistered.Load()
	}))
	expvar.Publish("evolution_runtimes_active", expvar.Func(func() interface{} {
		return runtimesActive.Load()
	}))
}

// SetClientsRegistered atualiza o gauge de clients registrados.
func SetClientsRegistered(n int) { clientsRegistered.Store(int64(n)) }

// SetRuntimesActive atualiza o gauge de runtimes ativas.
func SetRuntimesActive(n int) { runtimesActive.Store(int64(n)) }

// IncReconnect incrementa o contador de reconexões por resultado.
func IncReconnect(result string) { Reconnects.Add(result, 1) }

// IncDisconnected incrementa o contador de eventos de desconexão.
func IncDisconnected() { DisconnectedEvents.Add(1) }

// IncStreamError incrementa o contador de stream errors.
func IncStreamError() { StreamErrors.Add(1) }

// IncWebhookFailure incrementa o contador de webhooks com retries esgotados.
func IncWebhookFailure() { WebhookFailures.Add(1) }
