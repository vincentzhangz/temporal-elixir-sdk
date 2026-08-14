defmodule Temporal.Api.Sdk.V1.ExternalStorageReference.ClaimDataEntry do
  @moduledoc false

  use Protobuf,
    full_name: "temporal.api.sdk.v1.ExternalStorageReference.ClaimDataEntry",
    map: true,
    proto_source: "temporal/api/sdk/v1/external_storage.proto",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :key, 1, type: :string
  field :value, 2, type: :string
end

defmodule Temporal.Api.Sdk.V1.ExternalStorageReference do
  @moduledoc false

  use Protobuf,
    full_name: "temporal.api.sdk.v1.ExternalStorageReference",
    proto_source: "temporal/api/sdk/v1/external_storage.proto",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :driver_name, 1, type: :string, json_name: "driverName"

  field :claim_data, 2,
    repeated: true,
    type: Temporal.Api.Sdk.V1.ExternalStorageReference.ClaimDataEntry,
    json_name: "claimData",
    map: true
end
