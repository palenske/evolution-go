package whatsmeow_service

import (
	"sync"
	"testing"
	"time"
)

func TestRegistrySetGetDelete(t *testing.T) {
	r := NewClientRegistry()

	if got := r.GetClient("a"); got != nil {
		t.Fatalf("GetClient em registry vazio = %v, quer nil", got)
	}
	if _, ok := r.GetMyClient("a"); ok {
		t.Fatal("GetMyClient em registry vazio devolveu ok=true")
	}

	mycli := &MyClient{userID: "a"}
	r.SetMyClient("a", mycli)

	got, ok := r.GetMyClient("a")
	if !ok || got != mycli {
		t.Fatalf("GetMyClient = (%v, %v), quer (%v, true)", got, ok, mycli)
	}

	r.DeleteInstance("a")
	if _, ok := r.GetMyClient("a"); ok {
		t.Fatal("GetMyClient apos DeleteInstance devolveu ok=true")
	}

	// DeleteInstance e idempotente.
	r.DeleteInstance("a")
	r.DeleteInstance("inexistente")
}

func TestRegistryCountAndIDs(t *testing.T) {
	r := NewClientRegistry()
	if n := r.CountClients(); n != 0 {
		t.Fatalf("CountClients = %d, quer 0", n)
	}

	r.SetClient("a", nil)
	r.SetClient("b", nil)
	if n := r.CountClients(); n != 2 {
		t.Fatalf("CountClients = %d, quer 2", n)
	}

	ids := r.ClientIDs()
	if len(ids) != 2 {
		t.Fatalf("ClientIDs = %v, quer 2 entradas", ids)
	}

	// A copia devolvida nao pode afetar o registry.
	snap := r.SnapshotClients()
	delete(snap, "a")
	if n := r.CountClients(); n != 2 {
		t.Fatalf("SnapshotClients vazou o mapa interno: CountClients = %d", n)
	}
}

func TestBeginRuntimeExclusivo(t *testing.T) {
	r := NewClientRegistry()

	ctx, finish, ok := r.BeginRuntime("a")
	if !ok {
		t.Fatal("primeiro BeginRuntime devolveu ok=false")
	}
	if !r.HasRuntime("a") {
		t.Fatal("HasRuntime = false apos BeginRuntime")
	}

	if _, _, ok := r.BeginRuntime("a"); ok {
		t.Fatal("segundo BeginRuntime devolveu ok=true — reserva nao exclui")
	}

	// Outra instancia nao e afetada.
	if _, finishB, ok := r.BeginRuntime("b"); !ok {
		t.Fatal("BeginRuntime de outra instancia falhou")
	} else {
		finishB()
	}

	select {
	case <-ctx.Done():
		t.Fatal("ctx cancelado antes de Kill")
	default:
	}

	finish()

	if r.HasRuntime("a") {
		t.Fatal("HasRuntime = true apos finish()")
	}
	select {
	case <-ctx.Done():
	default:
		t.Fatal("finish() nao cancelou o ctx")
	}

	// Apos finish, a instancia pode ser reservada de novo.
	if _, finish2, ok := r.BeginRuntime("a"); !ok {
		t.Fatal("BeginRuntime apos finish devolveu ok=false")
	} else {
		finish2()
	}
}

func TestFinishIdempotente(t *testing.T) {
	r := NewClientRegistry()
	_, finish, _ := r.BeginRuntime("a")
	finish()
	finish() // nao pode entrar em panico com "close of closed channel"
}

func TestFinishNaoRemoveRuntimeSucessora(t *testing.T) {
	r := NewClientRegistry()

	_, finishVelha, _ := r.BeginRuntime("a")
	finishVelha()

	_, finishNova, ok := r.BeginRuntime("a")
	if !ok {
		t.Fatal("nao conseguiu reservar a runtime sucessora")
	}

	// A runtime antiga chamando finish de novo nao pode derrubar a sucessora.
	finishVelha()
	if !r.HasRuntime("a") {
		t.Fatal("finish da runtime antiga removeu a reserva da sucessora")
	}
	finishNova()
}

func TestKillCancelaEEsperaDone(t *testing.T) {
	r := NewClientRegistry()

	ctx, finish, ok := r.BeginRuntime("a")
	if !ok {
		t.Fatal("BeginRuntime falhou")
	}

	parou := make(chan struct{})
	go func() {
		<-ctx.Done()
		finish()
		close(parou)
	}()

	done := r.Kill("a")
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Kill: done nao fechou dentro de 2s")
	}

	select {
	case <-parou:
	case <-time.After(2 * time.Second):
		t.Fatal("goroutine de runtime nao retornou")
	}

	if r.HasRuntime("a") {
		t.Fatal("runtime continua reservada apos Kill")
	}
}

func TestKillSemRuntimeDevolveCanalFechado(t *testing.T) {
	r := NewClientRegistry()
	select {
	case <-r.Kill("inexistente"):
	case <-time.After(time.Second):
		t.Fatal("Kill sem runtime nao devolveu canal fechado")
	}
}

func TestKillIdempotente(t *testing.T) {
	r := NewClientRegistry()
	ctx, finish, _ := r.BeginRuntime("a")
	go func() {
		<-ctx.Done()
		finish()
	}()

	for i := 0; i < 3; i++ {
		r.Kill("a") // cancel repetido nao pode entrar em panico nem bloquear
	}
	if !r.WaitStopped("a", 2*time.Second) {
		t.Fatal("WaitStopped estourou o timeout")
	}
}

func TestWaitStoppedSemRuntime(t *testing.T) {
	r := NewClientRegistry()
	if !r.WaitStopped("inexistente", time.Millisecond) {
		t.Fatal("WaitStopped sem runtime = false, quer true")
	}
}

func TestKillAndWaitTimeout(t *testing.T) {
	r := NewClientRegistry()

	_, finish, _ := r.BeginRuntime("travada")
	defer finish()

	// Ninguem observa o ctx: a runtime nunca termina.
	if r.KillAndWait("travada", 50*time.Millisecond) {
		t.Fatal("KillAndWait = true para runtime que nao termina, quer false")
	}
}

func TestKillAndWaitOK(t *testing.T) {
	r := NewClientRegistry()
	ctx, finish, _ := r.BeginRuntime("a")
	go func() {
		<-ctx.Done()
		finish()
	}()

	if !r.KillAndWait("a", 2*time.Second) {
		t.Fatal("KillAndWait = false, quer true")
	}
}

func TestTryReconnectSingleflight(t *testing.T) {
	r := NewClientRegistry()

	release, ok := r.TryReconnect("a")
	if !ok {
		t.Fatal("primeiro TryReconnect devolveu ok=false")
	}

	if _, ok := r.TryReconnect("a"); ok {
		t.Fatal("segundo TryReconnect devolveu ok=true — singleflight nao exclui")
	}

	// Instancia diferente nao e bloqueada.
	if releaseB, ok := r.TryReconnect("b"); !ok {
		t.Fatal("TryReconnect de outra instancia foi bloqueado")
	} else {
		releaseB()
	}

	release()

	if release2, ok := r.TryReconnect("a"); !ok {
		t.Fatal("TryReconnect apos release devolveu ok=false")
	} else {
		release2()
	}
}

// TestRegistryConcorrente exercita todas as operacoes em paralelo; sob
// `go test -race` denuncia qualquer acesso nao sincronizado.
func TestRegistryConcorrente(t *testing.T) {
	r := NewClientRegistry()

	const goroutines = 24
	const iteracoes = 50
	ids := []string{"a", "b", "c"}

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			id := ids[g%len(ids)]
			for i := 0; i < iteracoes; i++ {
				switch (g + i) % 8 {
				case 0:
					r.SetClient(id, nil)
				case 1:
					r.GetClient(id)
				case 2:
					r.SetMyClient(id, &MyClient{userID: id})
				case 3:
					r.GetMyClient(id)
				case 4:
					r.DeleteInstance(id)
				case 5:
					r.CountClients()
				case 6:
					r.ClientIDs()
				case 7:
					if ctx, finish, ok := r.BeginRuntime(id); ok {
						go func() {
							<-ctx.Done()
							finish()
						}()
						r.KillAndWait(id, time.Second)
					}
				}
			}
		}(g)
	}
	wg.Wait()
}

// TestRuntimeUnicaSobReconexoesConcorrentes reproduz a mecanica do bug de leak:
// varias goroutines tentam "reconectar" a mesma instancia ao mesmo tempo
// (Kill + espera + BeginRuntime). No fim tem de sobrar exatamente uma runtime
// viva e nenhuma goroutine presa.
func TestRuntimeUnicaSobReconexoesConcorrentes(t *testing.T) {
	r := NewClientRegistry()
	const id = "instancia"

	iniciar := func() {
		ctx, finish, ok := r.BeginRuntime(id)
		if !ok {
			return
		}
		go func() {
			defer finish()
			<-ctx.Done()
		}()
	}

	iniciar()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if !r.KillAndWait(id, 5*time.Second) {
				t.Error("KillAndWait estourou o timeout — goroutine de runtime travada")
				return
			}
			iniciar()
		}()
	}
	wg.Wait()

	if !r.HasRuntime(id) {
		t.Fatal("nenhuma runtime viva no fim")
	}

	// Uma unica reserva: se houvesse runtime duplicada, BeginRuntime aqui
	// passaria depois de um unico KillAndWait.
	if !r.KillAndWait(id, 5*time.Second) {
		t.Fatal("KillAndWait final estourou o timeout")
	}
	if r.HasRuntime(id) {
		t.Fatal("sobrou runtime viva apos o Kill final — runtime duplicada")
	}
}
