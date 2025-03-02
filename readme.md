from(bucket: "datastorm")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "datastorm_metrics")