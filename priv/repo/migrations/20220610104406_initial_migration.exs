defmodule ArkePostgres.Repo.Migrations.InitialMigration do
  use Ecto.Migration

  def change do
    create table(:arke_unit, primary_key: false) do
      add :id, :string, primary_key: true
      add :arke_id, :string, null: false
      add :data, :map, default: %{}, null: false
      add :metadata, :map, default: %{}, null: false
      timestamps()
    end

    create table(:arke_link, primary_key: false) do
      add :type, :string, default: "link", null: false
      add :parent_id, references(:arke_unit, column: :id, type: :string, on_delete: :delete_all), primary_key: true
      add :child_id, references(:arke_unit, column: :id, type: :string, on_delete: :delete_all), primary_key: true
      add :metadata, :map, default: %{}, primary_key: true
    end

    create index(:arke_link, :parent_id)
    create index(:arke_link, :child_id)
    create index(:arke_link, :metadata)


    ## AUTH
    create table(:arke_auth, primary_key: false) do
      add :type, :map, default: %{read: true, write: true, delete: false}, null: false
      add :parent_id, references(:arke_unit, column: :id, type: :string, on_delete: :delete_all), primary_key: true
      add :child_id, references(:arke_unit, column: :id, type: :string, on_delete: :delete_all), primary_key: true
      add :metadata, :map, default: %{}
    end

    create index(:arke_auth, :parent_id)
    create index(:arke_auth, :child_id)
    create index(:arke_auth, :metadata)
  end
end
