# EcoSync Sentinel - Configuration File (config.py)
# Copy this to your project and import in main.py for environment-specific settings

from typing import Dict, Any

# ============================================================================
# DEVELOPMENT ENVIRONMENT
# ============================================================================
DEVELOPMENT_CONFIG: Dict[str, Any] = {
    "data_directory": "./data/",
    "api_host": "127.0.0.1",
    "api_port": 8000,
    "log_level": "DEBUG",
    
    # Anomaly Detection
    "temperature_multiplier": 1.2,
    "vibration_threshold": 0.8,
    
    # Windowing
    "window_size_minutes": 10,
    "window_step_minutes": 1,
    
    # Sustainability
    "co2_emission_factor": 0.475,  # kg CO2/kWh
    
    # Performance
    "max_memory_mb": 512,
    "cache_enabled": False,
}

# ============================================================================
# PRODUCTION ENVIRONMENT
# ============================================================================
PRODUCTION_CONFIG: Dict[str, Any] = {
    "data_directory": "/var/data/ecosync/",
    "api_host": "0.0.0.0",
    "api_port": 8000,
    "log_level": "INFO",
    
    # Stricter anomaly thresholds in production
    "temperature_multiplier": 1.15,
    "vibration_threshold": 0.75,
    
    # Larger windows for production stability
    "window_size_minutes": 15,
    "window_step_minutes": 2,
    
    # Grid-specific CO2 factor (update for your region)
    "co2_emission_factor": 0.475,
    
    # Performance optimizations
    "max_memory_mb": 2048,
    "cache_enabled": True,
    
    # Persistence
    "database_url": "postgresql://user:pass@localhost/ecosync",
    "persist_alerts": True,
    "persist_emissions": True,
}

# ============================================================================
# STAGING ENVIRONMENT
# ============================================================================
STAGING_CONFIG: Dict[str, Any] = {
    "data_directory": "/staging/data/",
    "api_host": "0.0.0.0",
    "api_port": 8000,
    "log_level": "INFO",
    
    "temperature_multiplier": 1.2,
    "vibration_threshold": 0.8,
    
    "window_size_minutes": 10,
    "window_step_minutes": 1,
    
    "co2_emission_factor": 0.475,
    
    "max_memory_mb": 1024,
    "cache_enabled": True,
}

# ============================================================================
# INDUSTRY-SPECIFIC PRESETS
# ============================================================================

TEXTILE_INDUSTRY: Dict[str, Any] = {
    """
    Configuration optimized for textile manufacturing:
    - High vibration from looms
    - Temperature-sensitive processes
    - Energy-intensive dyeing/finishing
    """
    "temperature_multiplier": 1.25,
    "vibration_threshold": 0.85,
    "window_size_minutes": 15,
    "co2_emission_factor": 0.520,  # Textile industry typical
}

AUTOMOTIVE_ASSEMBLY: Dict[str, Any] = {
    """
    Configuration optimized for automotive assembly:
    - Robotics with precise temperature control
    - Lower vibration tolerance
    - High energy demand
    """
    "temperature_multiplier": 1.1,
    "vibration_threshold": 0.6,
    "window_size_minutes": 5,
    "co2_emission_factor": 0.425,  # Industrial zone typical
}

ELECTRONICS_MANUFACTURING: Dict[str, Any] = {
    """
    Configuration optimized for electronics manufacturing:
    - Precision equipment requiring low vibration
    - Stable temperature zones
    - Lower energy per machine but many machines
    """
    "temperature_multiplier": 1.05,
    "vibration_threshold": 0.5,
    "window_size_minutes": 20,
    "co2_emission_factor": 0.475,
}

PHARMACEUTICAL_MANUFACTURING: Dict[str, Any] = {
    """
    Configuration for pharmaceutical manufacturing:
    - Strict temperature and vibration control
    - Compliance-heavy monitoring
    - Sensitive equipment
    """
    "temperature_multiplier": 1.02,
    "vibration_threshold": 0.3,
    "window_size_minutes": 30,
    "co2_emission_factor": 0.475,
}

# ============================================================================
# CO2 EMISSION FACTORS BY REGION (kg CO2/kWh)
# ============================================================================
CO2_FACTORS_BY_REGION: Dict[str, float] = {
    "North America": 0.425,
    "Europe": 0.350,
    "India": 0.625,
    "China": 0.700,
    "Brazil": 0.150,  # Hydroelectric heavy
    "Australia": 0.800,  # Coal heavy
    "Global Average": 0.475,
}

# ============================================================================
# HELPER FUNCTION
# ============================================================================

def get_config(environment: str = "development", 
               industry: str = None,
               region: str = "Global Average") -> Dict[str, Any]:
    """
    Get configuration for specified environment and industry.
    
    Args:
        environment: "development", "staging", or "production"
        industry: Optional industry preset to override
        region: Region for CO2 factor lookup
        
    Returns:
        Configuration dictionary
    """
    
    # Base environment config
    if environment.lower() == "production":
        config = PRODUCTION_CONFIG.copy()
    elif environment.lower() == "staging":
        config = STAGING_CONFIG.copy()
    else:
        config = DEVELOPMENT_CONFIG.copy()
    
    # Apply industry preset if specified
    if industry:
        industry_upper = industry.upper().replace(" ", "_")
        industry_config = globals().get(industry_upper)
        if industry_config:
            config.update(industry_config)
    
    # Apply regional CO2 factor
    config["co2_emission_factor"] = CO2_FACTORS_BY_REGION.get(region, 0.475)
    
    return config


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    # Get production config
    prod_config = get_config("production", "textile", "India")
    print("Production Textile Config (India):")
    for key, value in prod_config.items():
        print(f"  {key}: {value}")
    
    print("\n" + "="*50 + "\n")
    
    # Get development config
    dev_config = get_config("development")
    print("Development Config:")
    for key, value in dev_config.items():
        print(f"  {key}: {value}")
