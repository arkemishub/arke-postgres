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

defmodule ArkePostgres.ArkeLink do
  import Ecto.Query, only: [from: 2]
  alias Arke.Utils.ErrorGenerator, as: Error

  def get_all(project, schema, fields, where \\ []) do
    query = from(ArkePostgres.Tables.ArkeLink, select: ^fields, where: ^where)
    ArkePostgres.Repo.all(query, prefix: project)
  end

  def get_by(project, schema, fields, where) do
    query = from(ArkePostgres.Tables.ArkeLink, select: ^fields, where: ^where)
    ArkePostgres.Repo.one(query, prefix: project)
  end

  def insert(project, schema, data, opts \\ []) do
    records =
      Enum.map(data, fn unit ->
        %{
          parent_id: Map.get(unit.data, :parent_id),
          child_id: Map.get(unit.data, :child_id),
          type: Map.get(unit.data, :type),
          metadata: Map.get(unit, :metadata)
        }
      end)

    case ArkePostgres.Repo.insert_all(ArkePostgres.Tables.ArkeLink, records,
           prefix: project,
           returning: [:child_id, :parent_id, :type]
         ) do
      {0, _} ->
        {:error, Error.create(:insert, "no records inserted")}

      {count, inserted} ->
        inserted_keys =
          Enum.map(inserted, fn link -> {link.type, link.parent_id, link.child_id} end)

        {valid, errors} =
          Enum.split_with(data, fn unit ->
            {unit.data[:type], unit.data[:parent_id], unit.data[:child_id]} in inserted_keys
          end)

        case opts[:bulk] do
          true -> {:ok, count, valid, errors}
          _ -> {:ok, List.first(valid)}
        end
    end
  end

  def update(project, schema, data, where \\ []) do
    query = from(ArkePostgres.Tables.ArkeLink, where: ^where, update: [set: ^data])
    ArkePostgres.Repo.update_all(query, [], prefix: project)
  end

  def delete(project, schema, unit_list) do
    query =
      from([a] in ArkePostgres.Tables.ArkeLink,
        where: a.id in ^Enum.map(unit_list, &Atom.to_string(&1.id))
      )

    IO.inspect(query)

    # case ArkePostgres.Repo.delete_all(query, prefix: project) do
    #   {0, nil} -> Error.create(:delete, "item not found")
    #   _ -> {:ok, nil}
    # end
  end
end
