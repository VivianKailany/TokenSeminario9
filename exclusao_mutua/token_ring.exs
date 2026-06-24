defmodule TokenRing.Helper do
  @moduledoc """
  Implementação do Helper do algoritmo Token Ring (Figuras 9.14 e 9.15).
  Este processo coordena o acesso à Seção Crítica garantindo a invariante DMUTEX.
  """
  use GenServer

  # --- API Cliente ---

  def start_link(id), do: GenServer.start_link(__MODULE__, id)

  @doc "Configura a topologia de anel definindo o usuário local e o próximo Helper."
  def set_neighbors(pid, user_pid, next_helper) do
    GenServer.cast(pid, {:setup, user_pid, next_helper})
  end

  @doc "Protocolo de entrada: sinaliza que o usuário deseja acessar o recurso compartilhado."
  def request_entry(pid), do: GenServer.cast(pid, :set_entry_flag)

  # --- Callbacks do Servidor (Gerenciamento de Estado) ---

  def init(id) do
    {:ok, %{
      id: id,
      user_pid: nil,
      next_helper: nil,
      wants_entry: false,
      holding_token_for_user: false
    }}
  end

  @doc """
  Trata as mensagens de controle recebidas pelo Helper.
  """
  def handle_info(msg, state)

  def handle_info(:receive_token, state) do
    IO.puts("Helper #{state.id} recebeu o bastão.")

    if state.wants_entry do
      # Protocolo de entrada: retém o bastão e dá permissão ao usuário
      IO.puts("Helper #{state.id}: Usuário quer entrar. Concedendo permissão...")
      send(state.user_pid, :go)
      # Mantém o bastão: User em CS implica Helper com bastão (Invariante DMUTEX)
      {:noreply, %{state | holding_token_for_user: true}}
    else
      # Se não houver pedido, encaminha o bastão conforme a lógica circular
      IO.puts("Helper #{state.id}: nenhum pedido pendente, encaminhando o bastão.")
      pass_token(state)
      {:noreply, state}
    end
  end

  def handle_info(:user_finished, state) do
    if state.holding_token_for_user do
      # Protocolo de saída: usuário liberou o recurso, o bastão pode seguir
      IO.puts("Helper #{state.id}: Usuário liberou o recurso.")
      pass_token(state)
      {:noreply, %{state | holding_token_for_user: false, wants_entry: false}}
    else
      {:noreply, state}
    end
  end

  def handle_cast({:setup, user_pid, next_helper}, state) do
    {:noreply, %{state | user_pid: user_pid, next_helper: next_helper}}
  end

  def handle_cast(:set_entry_flag, state) do
    {:noreply, %{state | wants_entry: true}}
  end

  # --- Função Privada ---

  defp pass_token(state) do
    IO.puts("Helper #{state.id} passou o bastão.")
    # Atraso de software recomendado para evitar saturação e melhorar visualização
    Process.sleep(500)
    send(state.next_helper, :receive_token)
  end
end

defmodule TokenRing.User do
  @moduledoc "Representa o processo de aplicação que alterna entre seções críticas e não críticas."

  def start(helper_pid, id) do
    spawn(fn -> loop(helper_pid, id) end)
  end

  defp loop(helper_pid, id) do
    # 1. Seção Não Crítica (Simula processamento local)
    Process.sleep(Enum.random(2000..6000))

    # 2. Protocolo de Entrada: Solicita acesso ao seu Helper
    IO.puts("Usuário #{id} aguardando bastão...")
    TokenRing.Helper.request_entry(helper_pid)

    receive do
      :go ->
        # 3. SEÇÃO CRÍTICA (Acesso exclusivo garantido pela posse do bastão)
        IO.puts(">>> SUCESSO: Usuário #{id} está na SEÇÃO CRÍTICA!")
        Process.sleep(2000)

        # 4. Protocolo de Saída: Notifica o término do uso do recurso
        send(helper_pid, :user_finished)
        IO.puts("<<< Usuário #{id} saiu da seção crítica.")
    end

    loop(helper_pid, id)
  end
end

# --- Inicialização do Sistema ---

# Criar os processos auxiliares (Helpers) que formam o anel
{:ok, h1} = TokenRing.Helper.start_link(1)
{:ok, h2} = TokenRing.Helper.start_link(2)
{:ok, h3} = TokenRing.Helper.start_link(3)

# Criar os processos de usuário vinculados aos seus respectivos auxiliares
u1 = TokenRing.User.start(h1, 1)
u2 = TokenRing.User.start(h2, 2)
u3 = TokenRing.User.start(h3, 3)

# Configurar a topologia em anel: Helper 1 -> 2 -> 3 -> 1
TokenRing.Helper.set_neighbors(h1, u1, h2)
TokenRing.Helper.set_neighbors(h2, u2, h3)
TokenRing.Helper.set_neighbors(h3, u3, h1)

IO.puts("Iniciando sistema distribuído com bastão único...")
# Injetar o bastão inicial para começar a circulação
send(h1, :receive_token)

Process.sleep(:infinity)
