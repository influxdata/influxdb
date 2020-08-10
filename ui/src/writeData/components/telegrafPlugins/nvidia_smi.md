# `nvidia-smi` Input Plugin

This plugin uses a query on the [`nvidia-smi`](https://developer.nvidia.com/nvidia-system-management-interface) binary to pull GPU stats including memory and GPU usage, temp and other.

### Configuration

```toml
# Pulls statistics from nvidia GPUs attached to the host
[[inputs.nvidia_smi]]
  ## Optional: path to nvidia-smi binary, defaults to $PATH via exec.LookPath
  # bin_path = "/usr/bin/nvidia-smi"

  ## Optional: timeout for GPU polling
  # timeout = "5s"
```

#### Windows

On Windows, `nvidia-smi` is generally located at `C:\Program Files\NVIDIA Corporation\NVSMI\nvidia-smi.exe`
On Windows 10, you may also find this located here `C:\Windows\System32\nvidia-smi.exe`

You'll need to escape the `\` within the `telegraf.conf` like this: `C:\\Program Files\\NVIDIA Corporation\\NVSMI\\nvidia-smi.exe`

### Metrics
- measurement: `nvidia_smi`
  - tags
    - `name` (type of GPU e.g. `GeForce GTX 1070 Ti`)
    - `compute_mode` (The compute mode of the GPU e.g. `Default`)
    - `index` (The port index where the GPU is connected to the motherboard e.g. `1`)
    - `pstate` (Overclocking state for the GPU e.g. `P0`)
    - `uuid` (A unique identifier for the GPU e.g. `GPU-f9ba66fc-a7f5-94c5-da19-019ef2f9c665`)
  - fields
    - `fan_speed` (integer, percentage)
    - `fbc_stats_session_count` (integer)
    - `fbc_stats_average_fps` (integer)
    - `fbc_stats_average_latency` (integer)
    - `memory_free` (integer, MiB)
    - `memory_used` (integer, MiB)
    - `memory_total` (integer, MiB)
    - `power_draw` (float, W)
    - `temperature_gpu` (integer, degrees C)
    - `utilization_gpu` (integer, percentage)
    - `utilization_memory` (integer, percentage)
    - `utilization_encoder` (integer, percentage)
    - `utilization_decoder` (integer, percentage)
    - `pcie_link_gen_current` (integer)
    - `pcie_link_width_current` (integer)
    - `encoder_stats_session_count` (integer)
    - `encoder_stats_average_fps` (integer)
    - `encoder_stats_average_latency` (integer)
    - `clocks_current_graphics` (integer, MHz)
    - `clocks_current_sm` (integer, MHz)
    - `clocks_current_memory` (integer, MHz)
    - `clocks_current_video` (integer, MHz)

### Sample Query

The below query could be used to alert on the average temperature of the your GPUs over the last minute

```sql
SELECT mean("temperature_gpu") FROM "nvidia_smi" WHERE time > now() - 5m GROUP BY time(1m), "index", "name", "host"
```

### Troubleshooting

Check the full output by running `nvidia-smi` binary manually.

Linux:
```sh
sudo -u telegraf -- /usr/bin/nvidia-smi -q -x
```

Windows:
```
"C:\Program Files\NVIDIA Corporation\NVSMI\nvidia-smi.exe" -q -x
```

Please include the output of this command if opening an GitHub issue.

### Example Output
```
nvidia_smi,compute_mode=Default,host=8218cf,index=0,name=GeForce\ GTX\ 1070,pstate=P2,uuid=GPU-823bc202-6279-6f2c-d729-868a30f14d96 fan_speed=100i,memory_free=7563i,memory_total=8112i,memory_used=549i,temperature_gpu=53i,utilization_gpu=100i,utilization_memory=90i 1523991122000000000
nvidia_smi,compute_mode=Default,host=8218cf,index=1,name=GeForce\ GTX\ 1080,pstate=P2,uuid=GPU-f9ba66fc-a7f5-94c5-da19-019ef2f9c665 fan_speed=100i,memory_free=7557i,memory_total=8114i,memory_used=557i,temperature_gpu=50i,utilization_gpu=100i,utilization_memory=85i 1523991122000000000
nvidia_smi,compute_mode=Default,host=8218cf,index=2,name=GeForce\ GTX\ 1080,pstate=P2,uuid=GPU-d4cfc28d-0481-8d07-b81a-ddfc63d74adf fan_speed=100i,memory_free=7557i,memory_total=8114i,memory_used=557i,temperature_gpu=58i,utilization_gpu=100i,utilization_memory=86i 1523991122000000000
```

### Limitations
Note that there seems to be an issue with getting current memory clock values when the memory is overclocked.
This may or may not apply to everyone but it's confirmed to be an issue on an EVGA 2080 Ti.
