defmodule Temporal.Workflow.Update.Dispatcher do
  @moduledoc """
  Deterministic Update handler registry for Workflow code.

  Update handlers receive `(decoded_args, context)` and must return
  `{:ok, result}` or `{:error, reason}`. They run in Workflow context during
  the Workflow Task that carries the update request message and may read
  deterministic Workflow state but must not emit commands.
  """

  defstruct handlers: %{}

  @type handler :: (term(), map() -> {:ok, term()} | {:error, term()})
  @type t :: %__MODULE__{handlers: %{optional(String.t()) => handler()}}

  @spec new() :: t()
  def new, do: %__MODULE__{}

  @spec register(t(), String.t(), handler()) :: {:ok, t()} | {:error, term()}
  def register(%__MODULE__{} = dispatcher, update_name, handler)
      when is_binary(update_name) and update_name != "" and is_function(handler, 2) do
    {:ok, %{dispatcher | handlers: Map.put(dispatcher.handlers, update_name, handler)}}
  end

  def register(%__MODULE__{}, update_name, _handler),
    do: {:error, {:invalid_update_handler_name, update_name}}

  @spec fetch(t(), String.t()) :: {:ok, handler()} | :error
  def fetch(%__MODULE__{handlers: handlers}, update_name),
    do: Map.fetch(handlers, update_name)

  @spec handler?(t(), String.t()) :: boolean()
  def handler?(%__MODULE__{handlers: handlers}, update_name),
    do: Map.has_key?(handlers, update_name)
end
