package webhook_producer

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	producer_interfaces "github.com/evolution-foundation/evolution-go/pkg/events/interfaces"
	logger_wrapper "github.com/evolution-foundation/evolution-go/pkg/logger"
	"github.com/evolution-foundation/evolution-go/pkg/metrics"
)

type webhookProducer struct {
	url           string
	loggerWrapper *logger_wrapper.LoggerManager
	// httpClient compartilhado: um por producer, com Timeout. O padrao antigo
	// criava um &http.Client{} por request (Transport novo, sem keep-alive e
	// sem Timeout) — um endpoint que segurasse a resposta travava a goroutine
	// para sempre e cada retry abria um pool de conexoes novo.
	httpClient *http.Client
	// semaforo limita a concorrencia de goroutines de envio: Produce e chamado
	// por evento, e sem teto o numero de goroutines cresce sem limite sob
	// carga de webhook.
	sema chan struct{}
}

func NewWebhookProducer(
	url string,
	loggerWrapper *logger_wrapper.LoggerManager,
) producer_interfaces.Producer {
	return &webhookProducer{
		url:           url,
		loggerWrapper: loggerWrapper,
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
		},
		// 64 envios simultaneos de webhook e mais que suficiente para 10
		// instancias; sob carga, o excesso espera na canal em vez de criar
		// goroutines ilimitadas.
		sema: make(chan struct{}, 64),
	}
}

func (p *webhookProducer) Produce(
	queueName string,
	payload []byte,
	webhookUrl string,
	userID string,
) error {
	splitQueue := strings.Split(queueName, ".")

	if len(splitQueue) < 2 {
		return nil
	}

	// Backoff exponencial entre retries: 30s, 60s, 120s, 240s (em vez de
	// 5 x 30s fixos). Mantem a semantica de 5 tentativas.
	baseRetry := 30 * time.Second

	if p.url != "" {
		p.dispatch(p.url, payload, userID, baseRetry)
	}
	if webhookUrl != "" {
		p.dispatch(webhookUrl, payload, userID, baseRetry)
	}

	return nil
}

// dispatch adquire uma vaga no semaforo e dispara o envio em goroutine. Se o
// semaforo estiver cheio, descarta o envio com log de erro — melhor perder um
// webhook sob carga extrema do que acumular goroutines ate o OOM.
func (p *webhookProducer) dispatch(url string, body []byte, userID string, baseRetry time.Duration) {
	select {
	case p.sema <- struct{}{}:
		go func() {
			defer func() { <-p.sema }()
			p.sendWebhookWithRetry(url, body, 5, baseRetry, userID)
		}()
	default:
		p.loggerWrapper.GetLogger(userID).LogError("[%s] webhook dispatch queue full, dropping event — url: %s", userID, url)
	}
}

func (p *webhookProducer) sendWebhookWithRetry(url string, body []byte, maxRetries int, baseRetry time.Duration, userID string) {
	for i := 0; i < maxRetries; i++ {
		err, responseBody, statusCode := p.sendWebhook(url, body, userID)
		if err == nil {
			p.loggerWrapper.GetLogger(userID).LogInfo("[%s] webhook sent successfully - url: %s, status: %d, response: %s", userID, url, statusCode, string(responseBody))
			return
		}
		p.loggerWrapper.GetLogger(userID).LogWarn("[%s] webhook failed - url: %s, attempt: %d, error: %v", userID, url, i+1, err)

		if i < maxRetries-1 {
			// Backoff exponencial: 30s, 60s, 120s, 240s.
			time.Sleep(baseRetry * time.Duration(1<<i))
		}
	}
	p.loggerWrapper.GetLogger(userID).LogError("[%s] webhook failed after maximum retries - url: %s", userID, url)
	metrics.IncWebhookFailure()
}

func (p *webhookProducer) sendWebhook(url string, body []byte, userID string) (error, []byte, int) {
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(body))
	if err != nil {
		return err, nil, 0
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return err, nil, 0
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("erro ao ler resposta: %v", err), nil, 0
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return errors.New("received non-2xx response: " + resp.Status), responseBody, resp.StatusCode
	}

	return nil, responseBody, resp.StatusCode
}

// CreateGlobalQueues não faz nada para webhook producer
func (p *webhookProducer) CreateGlobalQueues() error {
	return nil
}
