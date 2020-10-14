# RAS Input Plugin

The `RAS` plugin gathers and counts errors provided by [RASDaemon](https://github.com/mchehab/rasdaemon).

### Configuration

```toml
[[inputs.ras]]
  ## Optional path to RASDaemon sqlite3 database.
  ## Default: /var/lib/rasdaemon/ras-mc_event.db
  # db_path = ""
```

In addition `RASDaemon` runs, by default, with `--enable-sqlite3` flag. In case of problems with SQLite3 database please verify this is still a default option.

### Metrics

- ras
  - tags:
    - socket_id
  - fields:
    - memory_read_corrected_errors
    - memory_read_uncorrectable_errors
    - memory_write_corrected_errors
    - memory_write_uncorrectable_errors
    - cache_l0_l1_errors
    - tlb_instruction_errors
    - cache_l2_errors
    - upi_errors
    - processor_base_errors
    - processor_bus_errors
    - internal_timer_errors
    - smm_handler_code_access_violation_errors
    - internal_parity_errors
    - frc_errors
    - external_mce_errors
    - microcode_rom_parity_errors
    - unclassified_mce_errors

Please note that `processor_base_errors` is aggregate counter measuring the following MCE events:
- internal_timer_errors
- smm_handler_code_access_violation_errors
- internal_parity_errors
- frc_errors
- external_mce_errors
- microcode_rom_parity_errors
- unclassified_mce_errors

### Permissions

This plugin requires access to SQLite3 database from `RASDaemon`. Please make sure that user has required permissions to this database.

### Example Output

```
ras,host=ubuntu,socket_id=0 external_mce_base_errors=1i,frc_errors=1i,instruction_tlb_errors=5i,internal_parity_errors=1i,internal_timer_errors=1i,l0_and_l1_cache_errors=7i,memory_read_corrected_errors=25i,memory_read_uncorrectable_errors=0i,memory_write_corrected_errors=5i,memory_write_uncorrectable_errors=0i,microcode_rom_parity_errors=1i,processor_base_errors=7i,processor_bus_errors=1i,smm_handler_code_access_violation_errors=1i,unclassified_mce_base_errors=1i 1598867393000000000
ras,host=ubuntu level_2_cache_errors=0i,upi_errors=0i 1598867393000000000
```
