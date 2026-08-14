defmodule Temporal.Workflow.TaskKernel.MachineRegistry do
  @moduledoc """
  State-machine storage addressed by semantic command type and deterministic ID.
  """

  @type key :: {atom(), String.t()}
  @type dispatcher :: (term(), term(), :live | :replay ->
                         {:ok, term(), term()} | {:error, term()})
  @type t :: %{machines: %{optional(key()) => term()}, types: %{optional(atom()) => dispatcher()}}

  @spec new() :: t()
  def new, do: %{machines: %{}, types: %{}}

  @spec register_type(t(), atom(), dispatcher()) :: {:ok, t()} | {:error, term()}
  def register_type(%{types: types} = registry, type, dispatcher)
      when is_atom(type) and is_function(dispatcher, 3) do
    if Map.has_key?(types, type) do
      {:error, {:duplicate_machine_type, type}}
    else
      {:ok, %{registry | types: Map.put(types, type, dispatcher)}}
    end
  end

  @spec register(t(), atom(), String.t(), term()) :: {:ok, t()} | {:error, term()}
  def register(%{machines: machines} = registry, type, id, machine)
      when is_atom(type) and is_binary(id) and id != "" do
    key = {type, id}

    if Map.has_key?(machines, key) do
      {:error, {:duplicate_command_id, %{type: type, id: id}}}
    else
      {:ok, %{registry | machines: Map.put(machines, key, machine)}}
    end
  end

  @spec fetch(t(), atom(), String.t()) :: {:ok, term()} | :error
  def fetch(%{machines: machines}, type, id), do: Map.fetch(machines, {type, id})

  @spec dispatch(t(), atom(), String.t(), term(), :live | :replay) ::
          {:ok, t(), term()} | {:error, term()}
  def dispatch(%{machines: machines, types: types} = registry, type, id, event, mode)
      when mode in [:live, :replay] do
    with {:ok, dispatcher} <- fetch_type(types, type),
         {:ok, machine} <- fetch_machine(machines, type, id),
         {:ok, next_machine, resolution} <- dispatcher.(machine, event, mode) do
      {:ok, %{registry | machines: Map.put(machines, {type, id}, next_machine)}, resolution}
    end
  end

  defp fetch_type(types, type) do
    case Map.fetch(types, type) do
      {:ok, dispatcher} -> {:ok, dispatcher}
      :error -> {:error, {:unregistered_machine_type, type}}
    end
  end

  defp fetch_machine(machines, type, id) do
    case Map.fetch(machines, {type, id}) do
      {:ok, machine} -> {:ok, machine}
      :error -> {:error, {:unknown_machine, %{type: type, id: id}}}
    end
  end
end
