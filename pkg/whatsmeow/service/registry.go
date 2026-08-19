package whatsmeow_service

import (
	"context"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
)

// ClientRegistry e o dono unico do estado por instancia que antes vivia em tres
// maps nus (`clientPointer`, `myClientPointer`, `killChannel`) compartilhados
// sem sincronizacao entre whatsmeow_service, instance_service, send_service e os
// 8 servicos de leitura.
//
// Alem de serializar os acessos, o registry passa a controlar o ciclo de vida da
// goroutine de runtime de cada instancia via context: BeginRuntime reserva a
// instancia e devolve o ctx que a goroutine deve observar; Kill cancela esse ctx
// (idempotente, nunca bloqueia) e devolve um canal fechado quando a goroutine
// efetivamente retornou.
//
// Vive no mesmo pacote de propositio: guarda *MyClient, e um pacote separado
// criaria ciclo de import.
type ClientRegistry struct {
	mu        sync.RWMutex
	clients   map[string]*whatsmeow.Client
	myClients map[string]*MyClient
	runtimes  map[string]*instanceRuntime

	// reconnect faz singleflight de ReconnectClient por instancia.
	// As entradas nunca sao removidas: o custo e desprezivel e deletar abriria
	// corrida entre o delete e o TryLock de outra goroutine.
	reconnect sync.Map // instanceID -> *sync.Mutex
}

// instanceRuntime representa uma goroutine de runtime viva para uma instancia.
// A presenca da entrada em ClientRegistry.runtimes E a reserva — nao existe
// token separado.
type instanceRuntime struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{} // fechado quando a goroutine de runtime retorna
	once   sync.Once
}

func NewClientRegistry() *ClientRegistry {
	return &ClientRegistry{
		clients:   make(map[string]*whatsmeow.Client),
		myClients: make(map[string]*MyClient),
		runtimes:  make(map[string]*instanceRuntime),
	}
}

// ---------------------------------------------------------------------------
// Clients
// ---------------------------------------------------------------------------

// GetClient devolve o client da instancia, ou nil se nao houver.
func (r *ClientRegistry) GetClient(instanceID string) *whatsmeow.Client {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.clients[instanceID]
}

// LookupClient devolve o client e se ele existe, para quem precisa distinguir
// "ausente" de "presente e nil".
func (r *ClientRegistry) LookupClient(instanceID string) (*whatsmeow.Client, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c, ok := r.clients[instanceID]
	return c, ok
}

func (r *ClientRegistry) SetClient(instanceID string, client *whatsmeow.Client) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clients[instanceID] = client
}

// GetMyClient devolve o MyClient da instancia.
func (r *ClientRegistry) GetMyClient(instanceID string) (*MyClient, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	mc, ok := r.myClients[instanceID]
	return mc, ok
}

func (r *ClientRegistry) SetMyClient(instanceID string, mycli *MyClient) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.myClients[instanceID] = mycli
}

// DeleteInstance remove client e MyClient da instancia. Idempotente.
// Nao mexe na runtime: quem encerra runtime e Kill/finish.
func (r *ClientRegistry) DeleteInstance(instanceID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.clients, instanceID)
	delete(r.myClients, instanceID)
}

// CountClients devolve quantos clients estao registrados.
func (r *ClientRegistry) CountClients() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.clients)
}

// ClientIDs devolve uma copia das chaves registradas, para iteracao segura sem
// segurar o lock.
func (r *ClientRegistry) ClientIDs() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]string, 0, len(r.clients))
	for id := range r.clients {
		ids = append(ids, id)
	}
	return ids
}

// SnapshotClients devolve uma copia do mapa de clients, para iteracao segura.
func (r *ClientRegistry) SnapshotClients() map[string]*whatsmeow.Client {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]*whatsmeow.Client, len(r.clients))
	for id, c := range r.clients {
		out[id] = c
	}
	return out
}

// ---------------------------------------------------------------------------
// Runtime lifecycle
// ---------------------------------------------------------------------------

// BeginRuntime reserva a instancia para uma nova goroutine de runtime.
//
// Devolve (ctx, finish, true) se a reserva foi obtida; (nil, nil, false) se ja
// existe runtime viva para a instancia. A goroutine deve observar ctx.Done() e
// chamar finish() no retorno (via defer) — finish fecha o canal `done`, o que
// libera quem espera em Kill/WaitStopped, e so remove a reserva se ela ainda for
// a sua (comparacao por identidade), nunca a de uma runtime sucessora.
func (r *ClientRegistry) BeginRuntime(instanceID string) (context.Context, func(), bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, held := r.runtimes[instanceID]; held {
		return nil, nil, false
	}

	ctx, cancel := context.WithCancel(context.Background())
	rt := &instanceRuntime{ctx: ctx, cancel: cancel, done: make(chan struct{})}
	r.runtimes[instanceID] = rt

	finish := func() {
		rt.once.Do(func() {
			r.mu.Lock()
			if cur, ok := r.runtimes[instanceID]; ok && cur == rt {
				delete(r.runtimes, instanceID)
			}
			r.mu.Unlock()
			cancel()
			close(rt.done)
		})
	}

	return ctx, finish, true
}

// HasRuntime informa se existe runtime viva para a instancia.
func (r *ClientRegistry) HasRuntime(instanceID string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.runtimes[instanceID]
	return ok
}

// closedChan e devolvido por Kill quando nao ha runtime a matar.
var closedChan = func() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

// Kill pede o encerramento da runtime da instancia e devolve o canal que fecha
// quando a goroutine efetivamente retorna. Nunca bloqueia; cancelar duas vezes e
// inofensivo. Se nao ha runtime, devolve um canal ja fechado.
func (r *ClientRegistry) Kill(instanceID string) <-chan struct{} {
	r.mu.RLock()
	rt, ok := r.runtimes[instanceID]
	r.mu.RUnlock()
	if !ok {
		return closedChan
	}
	rt.cancel()
	return rt.done
}

// WaitStopped espera a runtime da instancia terminar. Devolve true se ela
// terminou (ou nem existia) dentro do timeout, false se o timeout estourou.
// Nao cancela nada — use Kill antes, ou KillAndWait.
func (r *ClientRegistry) WaitStopped(instanceID string, timeout time.Duration) bool {
	r.mu.RLock()
	rt, ok := r.runtimes[instanceID]
	r.mu.RUnlock()
	if !ok {
		return true
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-rt.done:
		return true
	case <-timer.C:
		return false
	}
}

// KillAndWait cancela a runtime e espera ela retornar, ate timeout.
// Devolve true se a runtime terminou, false se o timeout estourou.
func (r *ClientRegistry) KillAndWait(instanceID string, timeout time.Duration) bool {
	done := r.Kill(instanceID)

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

// ---------------------------------------------------------------------------
// Reconnect singleflight
// ---------------------------------------------------------------------------

// TryReconnect tenta obter o direito exclusivo de reconectar a instancia.
// Devolve (release, true) para quem ganhou — o chamador deve chamar release()
// (tipicamente via defer) — ou (nil, false) se ja ha reconexao em andamento.
func (r *ClientRegistry) TryReconnect(instanceID string) (func(), bool) {
	muAny, _ := r.reconnect.LoadOrStore(instanceID, &sync.Mutex{})
	mu := muAny.(*sync.Mutex)
	if !mu.TryLock() {
		return nil, false
	}
	return mu.Unlock, true
}
