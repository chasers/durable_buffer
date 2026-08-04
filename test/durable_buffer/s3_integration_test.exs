defmodule DurableBuffer.S3IntegrationTest do
  use ExUnit.Case, async: false

  alias DurableBuffer.Test.FakeS3

  setup :set_req_test_to_shared

  defp set_req_test_to_shared(context) do
    Req.Test.set_req_test_to_shared(context)
  end

  test "concurrent appends group-commit into few segment PUTs" do
    {:ok, store} = FakeS3.start_store()
    stub_name = :"fake_s3_#{System.unique_integer([:positive])}"

    Req.Test.stub(stub_name, fn conn ->
      if conn.method == "PUT" do
        Process.sleep(10)
      end

      FakeS3.call(conn, store)
    end)

    name = :"s3_buffer_#{System.unique_integer([:positive])}"

    start_supervised!(
      {DurableBuffer,
       name: name,
       partitions: 1,
       backend:
         {DurableBuffer.Backend.S3,
          bucket: "test-bucket", req_options: [plug: {Req.Test, stub_name}, retry: false]}}
    )

    tasks =
      for index <- 1..30 do
        Task.async(fn -> DurableBuffer.append(name, "key", "entry-#{index}") end)
      end

    assert Enum.all?(Task.await_many(tasks, 10_000), &(&1 == :ok))

    segments = map_size(FakeS3.objects(store))
    assert segments < 30

    entries = Enum.sort(Enum.to_list(DurableBuffer.stream(name, "key")))
    assert entries == Enum.sort(Enum.map(1..30, &"entry-#{&1}"))
  end
end
