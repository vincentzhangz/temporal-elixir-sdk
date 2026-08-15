ExUnit.start()

exclusions =
  if System.get_env("TEMPORAL_ADDRESS") do
    []
  else
    [live_server: true]
  end

exclusions =
  if System.get_env("TEMPORAL_CLOUD_ADDRESS") do
    exclusions
  else
    [{:cloud_live, true} | exclusions]
  end

if exclusions != [] do
  ExUnit.configure(exclude: exclusions)
end
