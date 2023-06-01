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
  def run(_args) do
    case ArkePostgres.check_env() do
      {:ok, _} ->
        Mix.shell().info(" \e[34m ---- Creating database ---- \e[0m")
        app = Mix.Project.config()[:app]
        Application.put_env(app, :ecto_repos, [ArkePostgres.Repo])
        Mix.Task.run("ecto.create")

        [:postgrex, :ecto_sql, :arke_auth, :arke]
        |> Enum.each(&Application.ensure_all_started/1)

        ArkePostgres.Repo.start_link()

        Mix.shell().info("\e[34m ---- Creating schema ---- \e[0m")
        ArkePostgres.create_project(%{arke_id: :arke_project, id: :arke_system})
        Mix.shell().info("\e[32m ---- Schema created ---- \e[0m")
        Mix.shell().info("\e[34m ---- Creating arke_system project ---- \e[0m")
        create_base_project()
        Mix.shell().info("\e[32m ---- Project created ---- \e[0m")
        Mix.shell().info("\e[34m ---- Creating default user ---- \e[0m")
        create_admin_user()
        Mix.shell().info("\e[32m ---- User created ---- \e[0m")
        :ok

      {:error, keys} ->
        ArkePostgres.print_missing_env(keys)
    end
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

  defp create_admin_user() do
    user_arke = ArkeManager.get(:user, :arke_system)

    QueryManager.create(:arke_system, user_arke, %{
      username: "admin",
      password: "admin",
      type: "super_admin"
    })
  end
end
