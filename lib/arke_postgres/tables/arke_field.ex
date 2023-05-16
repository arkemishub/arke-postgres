# Copyright 2023 Arkemis S.r.l.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

defmodule ArkePostgres.Tables.ArkeField do
  use Ecto.Schema
  import Ecto.Changeset

  @arke_field_fields ~w[id label type format metadata]a
  @timestamps ~w[inserted_at updated_at]a

  @primary_key {:id, :string, []}
  schema "arke_field" do
    field(:label, :string)
    field(:type, :string, default: "string")
    field(:format, :string, default: "attribute")
    field(:metadata, :map, default: %{})
    field(:is_primary, :boolean, default: false)

    timestamps()
  end

  def changeset(arke_field, args) do
    arke_field
    |> cast(args, @arke_field_fields ++ @timestamps)
    |> validate_required(@arke_field_fields ++ @timestamps)
  end
end
