package whatsmeow_service

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"
)

// runtimeFake imita a mecanica do StartClient sem tocar em rede, banco ou
// whatsmeow: reserva a runtime, bloqueia em ctx.Done() e libera a reserva no
// retorno. E exatamente esse contrato que o bug de leak violava — o StartClient
// antigo ficava girando num select com `default` + sleep de 1s e, quando o
// killChannel era removido do map, o case virava leitura de canal nil e a
// goroutine nunca mais retornava.
func runtimeFake(t *testing.T, r *ClientRegistry, id string, aoMorrer func()) bool {
	t.Helper()

	ctx, finish, ok := r.BeginRuntime(id)
	if !ok {
		return false
	}

	go func() {
		defer finish()
		<-ctx.Done()
		if aoMorrer != nil {
			aoMorrer()
		}
	}()
	return true
}

func TestReconnectNaoVazaGoroutine(t *testing.T) {
	r := NewClientRegistry()
	const id = "instancia"
	const ciclos = 20

	if !runtimeFake(t, r, id, nil) {
		t.Fatal("nao conseguiu iniciar a runtime inicial")
	}

	// Deixa a runtime inicial assentar antes de medir.
	time.Sleep(50 * time.Millisecond)
	base := runtime.NumGoroutine()

	// "Reconexao" reduzida a sua mecanica: derruba a runtime, espera ela morrer,
	// sobe outra. Se cada ciclo deixasse a goroutine anterior viva, a contagem
	// cresceria linearmente com os ciclos.
	for i := 0; i < ciclos; i++ {
		if !r.KillAndWait(id, 5*time.Second) {
			t.Fatalf("ciclo %d: runtime nao morreu em 5s", i)
		}
		if !runtimeFake(t, r, id, nil) {
			t.Fatalf("ciclo %d: BeginRuntime falhou apos KillAndWait — reserva vazada", i)
		}
	}

	if !r.KillAndWait(id, 5*time.Second) {
		t.Fatal("runtime final nao morreu em 5s")
	}

	// Da uma folga para o scheduler recolher as goroutines encerradas.
	var depois int
	for tentativa := 0; tentativa < 20; tentativa++ {
		runtime.Gosched()
		time.Sleep(25 * time.Millisecond)
		depois = runtime.NumGoroutine()
		if depois <= base {
			break
		}
	}

	// Tolerancia pequena: outras goroutines do processo de teste podem oscilar,
	// mas 20 ciclos vazados apareceriam como +20.
	if depois > base+3 {
		t.Fatalf("goroutines vazadas: base=%d depois=%d apos %d ciclos", base, depois, ciclos)
	}
}

func TestReconnectConcorrenteMantemUmaRuntime(t *testing.T) {
	r := NewClientRegistry()
	const id = "instancia"

	var mu sync.Mutex
	ativas := 0
	maxAtivas := 0

	iniciar := func() {
		ctx, finish, ok := r.BeginRuntime(id)
		if !ok {
			return
		}
		mu.Lock()
		ativas++
		if ativas > maxAtivas {
			maxAtivas = ativas
		}
		mu.Unlock()

		go func() {
			defer finish()
			<-ctx.Done()
			mu.Lock()
			ativas--
			mu.Unlock()
		}()
	}

	iniciar()

	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if !r.KillAndWait(id, 5*time.Second) {
				t.Error("KillAndWait estourou o timeout")
				return
			}
			iniciar()
		}()
	}
	wg.Wait()

	mu.Lock()
	max := maxAtivas
	mu.Unlock()

	if max > 1 {
		t.Fatalf("chegou a ter %d runtimes simultaneas para a mesma instancia, quer 1", max)
	}
	if !r.KillAndWait(id, 5*time.Second) {
		t.Fatal("runtime final nao morreu")
	}
}

// TestKillDuranteBeginRuntime cobre a corrida entre matar e recriar: nenhuma das
// duas operacoes pode deixar a instancia num estado onde BeginRuntime falha para
// sempre (a reserva vazada que causava "Runtime already active" permanente).
func TestKillDuranteBeginRuntime(t *testing.T) {
	r := NewClientRegistry()
	const id = "instancia"

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			runtimeFake(t, r, id, nil)
		}()
		go func() {
			defer wg.Done()
			r.Kill(id)
		}()
	}
	wg.Wait()

	if !r.KillAndWait(id, 5*time.Second) {
		t.Fatal("runtime remanescente nao morreu em 5s")
	}

	ctx, finish, ok := r.BeginRuntime(id)
	if !ok {
		t.Fatal("instancia ficou com reserva vazada: BeginRuntime falha apos todas as runtimes morrerem")
	}
	_ = ctx
	finish()
}

// TestRuntimeCtxCancelaConsumidores garante que quem observa o ctx da runtime
// (schedulePresenceUpdates, no codigo real) termina quando a runtime e morta.
func TestRuntimeCtxCancelaConsumidores(t *testing.T) {
	r := NewClientRegistry()
	const id = "instancia"

	ctx, finish, ok := r.BeginRuntime(id)
	if !ok {
		t.Fatal("BeginRuntime falhou")
	}

	pararam := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(ctx context.Context) {
			defer wg.Done()
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
				case <-ctx.Done():
					return
				}
			}
		}(ctx)
	}

	go func() {
		<-ctx.Done()
		finish()
		wg.Wait()
		close(pararam)
	}()

	r.Kill(id)

	select {
	case <-pararam:
	case <-time.After(5 * time.Second):
		t.Fatal("consumidores do runtimeCtx nao terminaram apos Kill")
	}
}
