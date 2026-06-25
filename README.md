# Algoritmos de passagem de token
# Token Ring — Exclusão Mútua Distribuída em Elixir

**Relatório técnico do estudo de caso sobre Algoritmos de passagem de token**

Implementação em Elixir do algoritmo de **anel de bastão (token ring)** para exclusão mútua distribuída, apresentado na seção **9.6.1  "Distributed Mutual Exclusion"** de Misra & Andrews, *Foundations of Multithreaded, Parallel, and Distributed Programming* (Figuras 9.14 e 9.15 do livro).

## Introdução

Um **token** é uma mensagem especial que circula entre processos. Ele serve para duas coisas bem diferentes:

1. **Conceder permissão** — só quem tem o token pode fazer algo (ex.: entrar numa seção crítica).
2. **Coletar informação global** — o token "viaja" recolhendo informação sobre o estado de todos os processos (ex.: descobrir se a computação inteira já terminou).

A seção 9.6 mostra **um exemplo de cada uso**:

| Subseção | Problema | Papel do token |
|---|---|---|
| 9.6.1 | Exclusão mútua distribuída | Permissão para entrar na seção crítica |
| 9.6.2 | Detecção de término (anel) | Coleta de estado global (quem está ocioso) |
| 9.6.3 | Detecção de término (grafo completo) | Mesma coisa, topologia mais difícil |

## Resumo 9.6.2 — Detecção de Término em um Anel

**Problema:** numa computação sequencial, detectar término é trivial (o processo parou). Numa computação **distribuída**, é difícil: mesmo que todo processador pareça ocioso num instante, pode haver mensagens **em trânsito** que vão reativar alguém.

**Definição de término (DTERM):**

> Todo processo está ocioso **e** não há mensagens em trânsito.

**Premissa desta subseção:** a comunicação entre os processos forma um **anel** — `T[1] → T[2] → ... → T[n] → T[1]`.

**A ideia:** um token especial (que não faz parte da computação) circula pelo mesmo anel de canais usados pelas mensagens normais. Como as mensagens são entregues em ordem (FIFO), o token "empurra" e "limpa" qualquer mensagem normal que esteja na frente dele.

**Esquema de cores:**
- **vermelho (red)** = "quente" (já esteve ativo desde a última vez que viu o token)
- **azul (blue)** = "frio" (ocioso continuamente desde que viu o token)

**Regra de decisão:** `T[1]` inicia o protocolo ficando azul e mandando o token. Se o token voltar para `T[1]` e ele **continuar azul**, então todo mundo ficou ocioso o tempo todo — término confirmado.

**Invariante global (RING):**

> Token está em `T[1]` ⇒ `T[1]...T[token+1]` são azuis **e** os canais entre eles estão vazios.

---

## Resumo 9.6.3 — Detecção de Término em um Grafo Completo

**Problema:** generalizar 9.6.2 para quando **qualquer** processo pode mandar mensagem para **qualquer** outro (grafo completo de comunicação), não só para o vizinho do anel.

**Por que é mais difícil:** mensagens podem "ultrapassar" o token por um caminho diferente. Exemplo do livro com 3 processos: o token vai `T[1]→T[2]→T[3]→T[1]`, mas `T[3]` pode mandar uma mensagem comum direto para `T[2]` **antes** do token voltar — `T[2]` parecia ocioso, mas não estava mais.

**A solução:** em vez de percorrer só as arestas do anel, o token percorre um **ciclo `C` que passa por toda aresta do grafo completo** (cada aresta pelo menos uma vez).

**Generalização das cores + um contador:**
- O token carrega um valor que conta quantos canais consecutivos (na ordem do ciclo `C`) estavam vazios.
- Um processo fica **vermelho** ao receber mensagem normal; fica **azul** de novo só ao receber o token de novo estando ocioso.

**Quando termina:** quando o contador do token atinge `nc` (tamanho do ciclo `C`), significa que o token já deu **duas voltas completas** sem nenhuma atividade — uma volta para tornar todos azuis, outra para confirmar.

**Invariante (GRAPH):**

> token tem valor `V` ⇒ os últimos `V` canais do ciclo estavam vazios **e** os últimos `V` processos a receber o token estavam azuis.

## 9.6.1 — Exclusão Mútua Distribuída (Anel de Tokens)

## Sumário

1. [O problema](#1-o-problema)
2. [A solução do livro](#2-a-solução-do-livro)
3. [Mapeamento de conceitos: livro → Elixir](#3-mapeamento-de-conceitos-livro--elixir)
4. [Arquitetura da implementação (`token_ring.exs`)](#4-arquitetura-da-implementação-token_ringexs)
5. [Como executar](#5-como-executar)
6. [Discussão / limitações conhecidas](#6-discussão--limitações-conhecidas)
7. [Referência](#7-referência)

---

## 1. O problema

Vários processos (`User[i]`) precisam acessar, em momentos diferentes, um recurso compartilhado, por exemplo, um link de comunicação exclusivo, sem que dois deles o façam ao mesmo tempo. Como não há memória compartilhada entre os processos, a solução clássica baseada em locks/semáforos locais não se aplica diretamente; a única forma de coordenação é a troca de mensagens.

O livro apresenta três estratégias para esse problema:

| Estratégia | Característica |
|---|---|
| Monitor ativo central | Simples, porém centralizada (ponto único de falha/gargalo) |
| Semáforos distribuídos com `broadcast` | Descentralizada, mas custosa em número de mensagens |
| **Anel de bastão (token ring)** | Descentralizada **e** barata em mensagens — foco deste trabalho |

## 2. A solução do livro

- Cada `User[i]` tem um processo auxiliar dedicado, `Helper[i]`.
- Os `Helper`s são organizados em **anel lógico**:

```
Helper[1] → Helper[2] → Helper[3] → ... → Helper[n] → Helper[1]
```

- Um único **bastão (token)** circula permanentemente por esse anel.
- Quando `Helper[i]` recebe o bastão, ele verifica, **sem bloquear**, se seu `User[i]` pediu para entrar na seção crítica:
  - **Se sim:** concede permissão (`go`), retém o bastão até o usuário sinalizar saída (`exit`/`user_finished`), e só então o repassa.
  - **Se não:** repassa o bastão imediatamente ao próximo `Helper` do anel.

**Invariante garantida (predicado DMUTEX):**

> `User[i]` está em sua seção crítica ⇒ `Helper[i]` possui o bastão **∧** existe exatamente um bastão em todo o sistema.

Como o número de bastões nunca varia, ele apenas passa de processo a processo, e só quem o possui pode autorizar a entrada na seção crítica, a exclusão mútua é uma **consequência estrutural do protocolo**, sem necessidade de contadores, locks ou variáveis compartilhadas adicionais.

**Justiça (fairness):** como o bastão está sempre em movimento, todo `User[i]` eventualmente o recebe e, se estiver esperando, eventualmente entra na seção crítica, desde que todo usuário que entra também saia em tempo finito.

## 3. Mapeamento de conceitos: livro → Elixir

O BEAM (a VM do Erlang/Elixir) é construído nativamente sobre processos leves e isolados que se comunicam **apenas por troca de mensagens**, exatamente o modelo de computação assumido pelo pseudocódigo do livro (`chan`, `send`, `receive`). Isso permite uma tradução quase direta dos conceitos:

| Conceito do livro (CSP/Promela-like) | Construção em Elixir |
|---|---|
| `chan token[1:n]()` | Mensagem `:receive_token` enviada via `send/2` |
| `chan enter[i](), exit[i]()` | Mensagens `:set_entry_flag` (cast no GenServer) e `:user_finished` |
| `Helper[i]` | Processo `GenServer` (`TokenRing.Helper`) |
| `User[i]` | Processo leve (`spawn/1`) em `TokenRing.User` |
| `not empty(enter[i])` (poll não bloqueante) | Flag de estado (`wants_entry`) verificada de forma síncrona no `handle_info/2`, sem bloquear o recebimento do bastão |

## 4. Arquitetura da implementação (`token_ring.exs`)

O arquivo contém dois módulos e um script de inicialização.

### 4.1 `TokenRing.Helper`

Implementado como um `GenServer`, mantém o estado:

```elixir
%{
  id: id,
  user_pid: nil,
  next_helper: nil,
  wants_entry: false,
  holding_token_for_user: false
}
```

- `set_neighbors/3` — define, via `cast`, quem é o `User` local e qual é o próximo `Helper` no anel (monta a topologia).
- `request_entry/1` — chamado pelo `User`; marca `wants_entry: true` via `cast`.
- `handle_info(:receive_token, state)` — disparado quando o bastão chega:
  - se `wants_entry`, envia `:go` ao usuário e **retém** o bastão (`holding_token_for_user: true`);
  - senão, repassa o bastão imediatamente.
- `handle_info(:user_finished, state)` — disparado quando o usuário libera o recurso; reseta as flags e repassa o bastão ao próximo `Helper`.
- `pass_token/1` — função privada que aplica um `Process.sleep(500)` (atraso intencional só para facilitar a visualização da demo) e envia `:receive_token` ao próximo `Helper`.

### 4.2 `TokenRing.User`

Processo simples (`spawn/1`) com um laço infinito de quatro fases:

1. **Seção não crítica** — `Process.sleep(Enum.random(2000..6000))`, simulando trabalho local.
2. **Entrada** — chama `request_entry/1` e bloqueia em `receive` esperando `:go`.
3. **Seção crítica** — imprime confirmação de acesso exclusivo e aguarda `Process.sleep(2000)`.
4. **Saída** — envia `:user_finished` ao próprio `Helper` e volta ao passo 1.

### 4.3 Script de inicialização

Ao final do arquivo, o script:

1. Cria três `Helper`s (`h1`, `h2`, `h3`) via `start_link/1`.
2. Cria três `User`s, cada um vinculado a um `Helper`.
3. Fecha o anel chamando `set_neighbors/3` em cada `Helper`, definindo o próximo da sequência (`h1 → h2 → h3 → h1`).
4. Injeta o bastão inicial com `send(h1, :receive_token)`.
5. Mantém o processo principal vivo com `Process.sleep(:infinity)`, já que os `User`s e `Helper`s rodam em processos próprios.

## 5. Como executar

Pré-requisito: Elixir instalado (`elixir --version`).

```bash
elixir token_ring.exs
```

A saída mostra, em tempo real, o bastão circulando entre os três `Helper`s e os `User`s entrando e saindo da seção crítica, sempre um por vez, nunca dois simultaneamente, exatamente como prevê a invariante DMUTEX.

Para testar com mais processos, basta duplicar o padrão de criação de `Helper`/`User` e ajustar a cadeia de `set_neighbors/3` para fechar o anel com `n` elementos.

## 6. Discussão 

- **Perda do bastão:** o protocolo, como descrito no livro e implementado aqui, assume que o bastão nunca se perde nem é duplicado. Se um `Helper` falhar enquanto detém o bastão, o sistema trava (nenhum outro `User` jamais entra na seção crítica). A implementação atual não trata recuperação de falhas; estender o protocolo com detecção de perda e regeneração do bastão pode ser um trabalho futuro.

## 7. Referência

MISRA, J.; ANDREWS, G. R. *Foundations of Multithreaded, Parallel, and Distributed Programming*. Addison-Wesley, 2000. Seção 9.6.1 — Distributed Mutual Exclusion (Figuras 9.14 e 9.15).
