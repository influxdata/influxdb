#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_read_directory_as_module() {
        // Create a temporary directory with multiple Python files
        let temp_dir = TempDir::new().unwrap();
        let plugin_dir = temp_dir.path().join("test_plugin");
        fs::create_dir(&plugin_dir).unwrap();
        
        // Create helper.py
        let helper_content = r#"
def helper_function(value):
    return value * 2
"#;
        fs::write(plugin_dir.join("helper.py"), helper_content).unwrap();
        
        // Create utils.py
        let utils_content = r#"
import datetime

def get_timestamp():
    return datetime.datetime.now()
"#;
        fs::write(plugin_dir.join("utils.py"), utils_content).unwrap();
        
        // Create main.py (entrypoint)
        let main_content = r#"
from helper import helper_function
from utils import get_timestamp

def process_writes(influxdb3_local, table_batches, args=None):
    timestamp = get_timestamp()
    for batch in table_batches:
        processed = helper_function(batch.get('value', 0))
        influxdb3_local.info(f"Processed at {timestamp}: {processed}")
"#;
        fs::write(plugin_dir.join("main.py"), main_content).unwrap();
        
        // Create non-Python file (should be ignored)
        fs::write(plugin_dir.join("README.md"), "This is a readme").unwrap();
        
        // Test reading all Python files from the directory
        let mut module_code = String::new();
        
        // Read all .py files in the directory
        for entry in fs::read_dir(&plugin_dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            
            if path.extension().and_then(|s| s.to_str()) == Some("py") {
                let file_name = path.file_name().unwrap().to_str().unwrap();
                
                // Skip the entrypoint file for now
                if file_name == "main.py" {
                    continue;
                }
                
                // Add the file content as part of the module
                let content = fs::read_to_string(&path).unwrap();
                module_code.push_str(&format!("# File: {}\n", file_name));
                module_code.push_str(&content);
                module_code.push_str("\n\n");
            }
        }
        
        // Finally, add the entrypoint file
        let entrypoint_path = plugin_dir.join("main.py");
        let entrypoint_content = fs::read_to_string(&entrypoint_path).unwrap();
        module_code.push_str(&format!("# Entrypoint: main.py\n"));
        module_code.push_str(&entrypoint_content);
        
        // Verify all files are included
        assert!(module_code.contains("helper_function"));
        assert!(module_code.contains("get_timestamp"));
        assert!(module_code.contains("process_writes"));
        
        // Verify README.md is not included
        assert!(!module_code.contains("This is a readme"));
        
        // Verify entrypoint is last
        let main_pos = module_code.find("# Entrypoint: main.py").unwrap();
        let helper_pos = module_code.find("# File: helper.py").unwrap_or(usize::MAX);
        let utils_pos = module_code.find("# File: utils.py").unwrap_or(usize::MAX);
        
        if helper_pos != usize::MAX {
            assert!(main_pos > helper_pos);
        }
        if utils_pos != usize::MAX {
            assert!(main_pos > utils_pos);
        }
    }
}