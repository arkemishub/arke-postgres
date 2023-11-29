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

defmodule Mix.Tasks.ArkePostgres.CreateProject do
  alias Arke.QueryManager
  alias Arke.Boundary.ArkeManager
  use Mix.Task

  @shortdoc "Creates a new project"

  @moduledoc """
  Creates a new project in the database.

  ## Examples

      $ mix arke_postgres.create_project --id my_project
      $ mix arke_postgres.create_project --id my_project --description "My project description"

  ## Command line options

  * `--id` - The id of the project to create
  * `--label` - The label of the project to create
  * `--description` - The description of the project to create
  """

  @impl true
  def run([]) do
    Mix.shell().error(
      "Project ID is missing. Please provide an id for the project by passing --id project_id"
    )
  end

  def run(args) do
    case ArkePostgres.check_env() do
      {:ok, _} ->
        [:postgrex, :ecto_sql, :arke]
        |> Enum.each(&Application.ensure_all_started/1)
        parsed_args = parse_args(args)
        check_app_name!(parsed_args)
        case ArkePostgres.Repo.start_link() do
          {:ok, pid} ->
            create_data(parsed_args)
            Process.exit(pid, :normal)
            :ok

          {:error, _} -> create_data(parsed_args)

        end

      {:error, keys} ->
        ArkePostgres.print_missing_env(keys)
    end
  end

  defp check_app_name!([{:id, project_id} | _] =opts) do
    unless project_id =~ Regex.recompile!(~r/^[a-z_]*$/) do

      Mix.raise(
        "Application name must have only lowercase letters and underscorres got: #{inspect(project_id)}"
      )
    end
  end

  defp create_data(parsed_args) do
    with {:ok, _project} <- create_project(parsed_args),
         {:ok, admin_unit} <- create_super_admin_arke(parsed_args),
         {:ok, _group_unit} <- create_member_group(parsed_args),
         {:ok, dynamic_unit} <- create_dynamic_arke(parsed_args),
         {:ok, dynamic_link} <- create_dynamic_link(admin_unit,dynamic_unit,parsed_args) do
      :ok
    else
      {:error,msg} ->
        IO.inspect(msg,syntax_colors: [atom: :cyan, string: :red])
        :ok

    end
  end

  defp create_project([{:id, project_id} | _] = opts) do
    arke_project = ArkeManager.get(:arke_project, :arke_system)

    QueryManager.create(:arke_system, arke_project,
      id: project_id,
      label: Keyword.get(opts, :label, String.capitalize(project_id)),
      description: Keyword.get(opts, :description, "Project #{project_id}"),
      type: "postgres_schema"
    )
  end

  defp create_project(_) do
    Mix.shell().error(
      "Project ID is missing. Please provide an id for the project by passing --id project_id"
    )
  end

  defp parse_args(args) do
    {options, _, _} =
      OptionParser.parse(args, strict: [id: :string, label: :string, description: :string])

    options
  end

  defp create_member_group([{:id, project_id} | _] = _opts) do
    arke_group = ArkeManager.get(:group,:arke_system)
    QueryManager.create(project_id, arke_group,
      id: :arke_auth_member,
      label: "Arke auth member",
      description: "Handle members with arke_auth",
      arke_list: ["super_admin"]
    )
  end

  defp create_super_admin_arke([{:id, project_id} | _] =_opts) do
    arke_model = ArkeManager.get(:arke,:arke_system)
    QueryManager.create(project_id, arke_model,
      id: :super_admin,
      label: "Super admin",
    )
  end

  defp create_dynamic_link(parent,child,[{:id, project_id} | _] =_opts) do
    arke_link = ArkeManager.get(:arke_link, :arke_system)
    QueryManager.create(project_id, arke_link,
      parent_id: Atom.to_string(parent.id),
      child_id: Atom.to_string(child.id),
      type: "parameter",
      metadata: %{}
    )
  end

  defp create_dynamic_arke([{:id, project_id} | _] =_opts) do
    dynamic_model = ArkeManager.get(:dynamic,:arke_system)
    QueryManager.create(project_id, dynamic_model,
      id: :arke_system_user,
      label: "Arke System User",
      arke_list: ["super_admin"]
    )
  end

end


