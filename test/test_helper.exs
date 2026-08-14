ExUnit.start()

unless System.get_env("TEMPORAL_ADDRESS") do
  ExUnit.configure(exclude: [live_server: true])
end
