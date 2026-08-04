defmodule DurableBuffer.MixProject do
  use Mix.Project

  def project do
    [
      app: :durable_buffer,
      version: "0.2.0",
      elixir: "~> 1.16",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {DurableBuffer.Application, []}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  defp deps do
    [
      {:req, "~> 0.5"},
      {:req_s3, "~> 0.2"},
      {:benchee, "~> 1.3", only: :dev},
      {:plug, "~> 1.16", only: [:dev, :test]}
    ]
  end
end
