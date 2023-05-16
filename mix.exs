defmodule ArkePostgres.MixProject do
  use Mix.Project

  def project do
    [
      app: :arke_postgres,
      version: "0.1.2",
      build_path: "./_build",
      config_path: "./config/config.exs",
      deps_path: "./deps",
      lockfile: "./mix.lock",
      elixir: "~> 1.13",
      description: description(),
      package: package(),
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {ArkePostgres.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    List.flatten([
      {:ecto_sql, "~> 3.8.3"},
      {:postgrex, ">= 0.0.0"},
      {:jason, "~> 1.2"},
      {:ex_doc, "~> 0.28", only: :dev, runtime: false},
      {:credo, "~> 1.6", only: [:dev, :test], runtime: false},
      {:excoveralls, "~> 0.10", only: :test},
      get_arke(Mix.env())

      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"},
    ])
  end

  defp get_arke(:dev) do
    [{:arke, in_umbrella: true}]
  end

  defp get_arke(_env) do
    [{:arke, "~> 0.1.2"}]
  end

  defp description() do
    "Arke postgres"
  end
  defp package() do
    [
      # This option is only needed when you don't want to use the OTP application name
      name: "arke_postgres",
      # These are the default files included in the package
      licenses: ["Apache-2.0"],
      links: %{}
    ]
  end
end
