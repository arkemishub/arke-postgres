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

defmodule ArkePostgres do
  alias ArkePostgres.{Table, ArkeUnit}

  def init() do
    case check_env(Mix.env()) do
      {:ok, nil} ->
        try do
          projects =
            Arke.QueryManager.query(arke: :arke_project, project: :arke_system)
            |> Arke.QueryManager.filter(:id, :eq, :arke_system, true)
            |> Arke.QueryManager.filter(:arke_id, :eq, "arke_project")
            |> Arke.QueryManager.all()

          Enum.each(projects, fn %{id: project_id} = _project ->
            start_managers(project_id)
          end)

          :ok
        rescue
          _ in DBConnection.ConnectionError -> :error
          _ in Postgrex.Error -> :error
        end

      {:error, keys} ->
        print_missing_env(keys)
        :error
    end
  end

  def print_missing_env(keys) when is_list(keys) do
    for k <- keys do
      IO.puts("#{IO.ANSI.red()} error:#{IO.ANSI.reset()} env key #{k} not found.")
    end
  end

  def print_missing_env(keys), do: print_missing_env([keys])

  def check_env(:test), do: {:ok, nil}

  def check_env(_env) do
    keys = ["DB_NAME", "DB_HOSTNAME", "DB_USER", "DB_PASSWORD"]

    key_map =
      Enum.reduce(keys, [], fn k, acc ->
        if System.get_env(k) == nil, do: [k | acc], else: acc
      end)

    with true <- length(key_map) != 0 do
      {:error, key_map}
    else
      _ -> {:ok, nil}
    end
  end

  defp start_managers(project_id) do
    {parameters, arke_list, groups} = ArkePostgres.Query.get_manager_units(project_id)

    arke = Arke.Boundary.ArkeManager.get(:arke, :arke_system)
    Enum.each(parameters, fn unit -> Arke.Boundary.ParameterManager.create(unit, project_id) end)

    Enum.each(arke_list, fn unit ->
      unit =
        Arke.Core.Unit.update(unit,
          parameters: unit.data.parameters ++ Arke.Core.Arke.base_parameters(arke)
        )

      Arke.Boundary.ArkeManager.create(unit, project_id)
    end)

    Enum.each(groups, fn unit -> Arke.Boundary.GroupManager.create(unit, project_id) end)
  end

  def create(project, %{arke_id: arke_id} = unit) do
    arke = Arke.Boundary.ArkeManager.get(arke_id, project)

    case handle_create(project, arke, unit) do
      {:ok, unit} ->
        {:ok,
         Arke.Core.Unit.update(unit, metadata: Map.merge(unit.metadata, %{project: project}))}

      {:error, errors} ->
        {:error, handle_changeset_errros(errors)}
    end
  end

  defp handle_create(
         project,
         %{data: %{type: "table"}} = arke,
         %{data: data, metadata: metadata} = unit
       ) do
    data = data |> Map.merge(%{metadata: metadata}) |> data_as_klist
    Table.insert(project, arke, data)
    {:ok, unit}
  end

  defp handle_create(project, %{data: %{type: "arke"}} = arke, unit) do
    case ArkeUnit.insert(project, arke, unit) do
      {:ok, %{id: id, inserted_at: inserted_at, updated_at: updated_at}} ->
        {:ok,
         Arke.Core.Unit.update(unit, id: id, inserted_at: inserted_at, updated_at: updated_at)}

      {:error, errors} ->
        {:error, errors}
    end
  end

  defp handle_create(_, _, _) do
    {:ok, "arke type not supported"}
  end

  def update(project, %{arke_id: arke_id} = unit) do
    arke = Arke.Boundary.ArkeManager.get(arke_id, project)
    {:ok, unit} = handle_update(project, arke, unit)
  end

  def handle_update(project, %{data: %{type: "table"}} = arke, unit) do
    data = unit |> filter_primary_keys(false) |> data_as_klist
    where = unit |> filter_primary_keys(true) |> data_as_klist

    case Table.update(project, arke, data, where) do
      {:ok, _} -> {:ok, unit}
      {:error, msg} -> {:error, msg}
    end
  end

  def handle_update(project, %{data: %{type: "arke"}} = arke, unit) do
    ArkeUnit.update(project, arke, unit)
    {:ok, unit}
  end

  def handle_update(_, _, _) do
    {:error, "arke type not supported"}
  end

  def delete(project, %{arke_id: arke_id} = unit) do
    arke = Arke.Boundary.ArkeManager.get(arke_id, project)
    handle_delete(project, arke, unit)
  end

  defp handle_delete(project, %{data: %{type: "table"}} = arke, %{metadata: metadata} = unit) do
    metadata = Map.delete(metadata, :project)

    where = unit |> filter_primary_keys(true) |> Map.put_new(:metadata, metadata) |> data_as_klist

    case Table.delete(project, arke, where) do
      {:ok, _} -> {:ok, nil}
      {:error, msg} -> {:error, msg}
    end
  end

  defp handle_delete(project, %{data: %{type: "arke"}} = arke, unit) do
    case ArkeUnit.delete(project, arke, unit) do
      {:ok, _} -> {:ok, nil}
      {:error, msg} -> {:error, msg}
    end
  end

  defp handle_delete(_, _, _) do
    {:error, "arke type not supported"}
  end

  defp filter_primary_keys(
         %{arke_id: arke_id, metadata: %{project: project}} = unit,
         is_primary \\ true
       ) do
    arke = Arke.Boundary.ArkeManager.get(arke_id, project)

    parameters =
      Enum.filter(Arke.Boundary.ArkeManager.get_parameters(arke), fn %{data: param_data} ->
        param_data.is_primary != is_primary
      end)

    unit.data |> remove_parameters(parameters)
  end

  defp remove_parameter(data, parameter) do
    Map.delete(data, parameter.id)
  end

  defp remove_parameters(data, parameters) do
    Enum.reduce(parameters, data, fn f, new_struct ->
      remove_parameter(new_struct, f)
    end)
  end

  def data_as_klist(data) do
    Enum.to_list(data)
  end

  defp handle_changeset_errros(errors) do
    Enum.map(errors, fn {field, detail} ->
      "#{field}: #{render_detail(detail)}"
    end)
  end

  defp render_detail({message, values}) do
    Enum.reduce(values, message, fn {k, v}, acc ->
      String.replace(acc, "%{#{k}}", to_string(v))
    end)
  end

  defp render_detail(message) do
    message
  end

  ######################################################################################################################

  def create_project(%{arke_id: :arke_project, id: id} = _unit) do
    try do
      sql = "CREATE SCHEMA \"#{id}\""
      Ecto.Adapters.SQL.query(ArkePostgres.Repo, sql, [])
      Ecto.Migrator.run(ArkePostgres.Repo, :up, all: true, prefix: id)
      :ok
    rescue
      _ in DBConnection.ConnectionError -> :error
      _ in Postgrex.Error -> :error
    end
  end

  # TODO handle exception
  def create_project(_), do: nil

  def delete_project(%{arke_id: :arke_project, id: id} = unit) do
    sql = "DROP SCHEMA \"#{id}\" CASCADE"
    Ecto.Adapters.SQL.query(ArkePostgres.Repo, sql, [])
  end

  # TODO handle exception
  def delete_project(_), do: nil
end
