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

defmodule Mix.Tasks.ArkePostgres.InitDb do
  alias Arke.QueryManager
  alias Arke.Boundary.ArkeManager
  use Mix.Task

  @shortdoc "Init new arke postgres DB"
  @switches [
    quiet: :boolean,
    username: :string,
    password: :string
  ]
  @aliases [
    q: :quiet,
    u: :username,
    pwd: :password
  ]

  @moduledoc """
  Create the essentials so you can start using arke.

  The essentials are created in the db specified under
  `:arke_postgres` option in the current app configuration.

  ## Examples

      $ mix arke_postgres.init_db


  ## Command line options


    * `--quiet` - do not log output
    * `--username` - do not create default_user
    * `--password` - do not create default_user


  """
  @impl true
  def run(args) do
    {opts, _} = OptionParser.parse!(args, strict: @switches, aliases: @aliases)

    case ArkePostgres.check_env() do
      {:ok, _} ->
        [:postgrex, :ecto_sql, :arke_auth, :arke]
        |> Enum.each(&Application.ensure_all_started/1)

        {:ok, pid} = ArkePostgres.Repo.start_link()

        unless opts[:quiet] do
          print_message("---- Creating schema ----")
        end

        ArkePostgres.create_project(%{arke_id: :arke_project, id: :arke_system})

        unless opts[:quiet] do
          print_message("---- Schema created ----")
          print_message("---- Creating arke_system project ----")
        end

        create_base_project()

        unless opts[:quiet] do
          print_message("---- Project created ----")
        end

        unless is_nil(opts[:username]) and is_nil(opts[:password]) do
          unless opts[:quiet] do
            print_message("---- Creating user #{opts[:username]} ----")
          end

          create_admin_user(opts[:username], opts[:password])

          unless opts[:quiet] do
            print_message("---- User created ----")
          end
        end

        unless opts[:quiet] do
          print_message("---- Creating default user ----")
        end

        create_admin_user("admin", "admin")

        unless opts[:quiet] do
          print_message("---- Default user created ----")
        end

        Process.exit(pid, :normal)
        :ok

      {:error, keys} ->
        ArkePostgres.print_missing_env(keys)
    end
  end

  defp print_message(msg) do
    Mix.shell().info(msg)
  end

  defp create_base_project() do
    arke_project = ArkeManager.get(:arke_project, :arke_system)

    QueryManager.create(:arke_system, arke_project,
      id: :arke_system,
      label: "Arke system",
      description: "Base project of the application",
      type: "postgres_schema"
    )
  end

  defp create_admin_user(username, password) do
    user_arke = ArkeManager.get(:user, :arke_system)

    QueryManager.create(:arke_system, user_arke, %{
      username: username,
      password: password,
      type: "super_admin"
    })
  end
end
