-- =============================================================================
-- security/row_access_policies.sql
-- Franchisee isolation: every user sees only their authorised location(s).
-- =============================================================================

USE ROLE SYSADMIN;

-- Mapping table: which locations each user can access
CREATE OR REPLACE TABLE security.user_location_access (
    user_id      VARCHAR(128) NOT NULL,
    location_id  VARCHAR(64)  NOT NULL,
    granted_by   VARCHAR(128) NOT NULL DEFAULT CURRENT_USER(),
    granted_at   TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE security.region_location_map (
    region_manager  VARCHAR(128) NOT NULL,
    location_id     VARCHAR(64)  NOT NULL
);

-- Row Access Policy: enforces franchisee isolation at query time
CREATE OR REPLACE ROW ACCESS POLICY security.franchise_isolation
  AS (location_id VARCHAR)
  RETURNS BOOLEAN ->
    CASE
      -- Corporate full access
      WHEN IS_ROLE_IN_SESSION('CORPORATE_ANALYST')  THEN TRUE
      WHEN IS_ROLE_IN_SESSION('DATA_ENGINEER')       THEN TRUE

      -- Regional manager: sees their territory
      WHEN IS_ROLE_IN_SESSION('REGIONAL_MANAGER')
           AND EXISTS (
             SELECT 1 FROM security.region_location_map
             WHERE region_manager = CURRENT_USER()
               AND location_id    = location_id
           ) THEN TRUE

      -- Franchisee: sees only their own locations
      WHEN IS_ROLE_IN_SESSION('FRANCHISEE_USER')
           AND EXISTS (
             SELECT 1 FROM security.user_location_access
             WHERE user_id     = CURRENT_USER()
               AND location_id = location_id
           ) THEN TRUE

      ELSE FALSE
    END;

-- Apply policy to every Gold table that contains location_id
ALTER TABLE gold.kitchen_efficiency  ADD ROW ACCESS POLICY security.franchise_isolation ON (location_id);
ALTER TABLE gold.dynamic_pricing     ADD ROW ACCESS POLICY security.franchise_isolation ON (location_id);
ALTER TABLE gold.prep_lists          ADD ROW ACCESS POLICY security.franchise_isolation ON (location_id);
ALTER TABLE ops.orders_realtime      ADD ROW ACCESS POLICY security.franchise_isolation ON (location_id);


-- =============================================================================
-- security/masking_policies.sql
-- Column-level security: hide sensitive values from non-authorised roles.
-- =============================================================================

-- Payment amounts: visible only to finance roles
CREATE OR REPLACE MASKING POLICY security.payment_amount_mask
  AS (val NUMBER(10, 2))
  RETURNS NUMBER(10, 2) ->
    CASE
      WHEN IS_ROLE_IN_SESSION('CORPORATE_FINANCE')  THEN val
      WHEN IS_ROLE_IN_SESSION('FRANCHISEE_OWNER')   THEN val
      ELSE NULL
    END;

-- Customer PII: email visible to CRM roles only
CREATE OR REPLACE MASKING POLICY security.email_mask
  AS (val VARCHAR)
  RETURNS VARCHAR ->
    CASE
      WHEN IS_ROLE_IN_SESSION('CRM_ANALYST')         THEN val
      WHEN IS_ROLE_IN_SESSION('CORPORATE_ANALYST')   THEN val
      ELSE '****@****.***'
    END;

-- Apply masking policies
ALTER TABLE ops.orders_realtime        MODIFY COLUMN total_amount SET MASKING POLICY security.payment_amount_mask;
ALTER TABLE silver.customer_profiles   MODIFY COLUMN email        SET MASKING POLICY security.email_mask;


-- =============================================================================
-- security/role_hierarchy.sql
-- =============================================================================

USE ROLE SECURITYADMIN;

-- Roles
CREATE ROLE IF NOT EXISTS CORPORATE_ANALYST;
CREATE ROLE IF NOT EXISTS CORPORATE_FINANCE;
CREATE ROLE IF NOT EXISTS REGIONAL_MANAGER;
CREATE ROLE IF NOT EXISTS FRANCHISEE_OWNER;
CREATE ROLE IF NOT EXISTS FRANCHISEE_USER;
CREATE ROLE IF NOT EXISTS CRM_ANALYST;
CREATE ROLE IF NOT EXISTS DATA_ENGINEER;
CREATE ROLE IF NOT EXISTS KITCHEN_STAFF;

-- Hierarchy (child → parent)
GRANT ROLE FRANCHISEE_USER     TO ROLE FRANCHISEE_OWNER;
GRANT ROLE FRANCHISEE_OWNER    TO ROLE REGIONAL_MANAGER;
GRANT ROLE REGIONAL_MANAGER    TO ROLE CORPORATE_ANALYST;
GRANT ROLE CORPORATE_FINANCE   TO ROLE CORPORATE_ANALYST;
GRANT ROLE CRM_ANALYST         TO ROLE CORPORATE_ANALYST;
GRANT ROLE CORPORATE_ANALYST   TO ROLE SYSADMIN;
GRANT ROLE DATA_ENGINEER       TO ROLE SYSADMIN;

-- Warehouse grants
GRANT USAGE ON WAREHOUSE KITCHEN_ANALYTICS_WH  TO ROLE KITCHEN_STAFF;
GRANT USAGE ON WAREHOUSE KITCHEN_ANALYTICS_WH  TO ROLE FRANCHISEE_USER;
GRANT USAGE ON WAREHOUSE BI_REPORTING_WH        TO ROLE CORPORATE_ANALYST;
GRANT USAGE ON WAREHOUSE BI_REPORTING_WH        TO ROLE FRANCHISEE_OWNER;
GRANT USAGE ON WAREHOUSE DATA_ENGINEERING_WH    TO ROLE DATA_ENGINEER;

-- Schema grants
GRANT USAGE ON DATABASE RESTAURANT_DB               TO ROLE FRANCHISEE_USER;
GRANT USAGE ON SCHEMA   RESTAURANT_DB.GOLD          TO ROLE FRANCHISEE_USER;
GRANT SELECT ON ALL TABLES IN SCHEMA RESTAURANT_DB.GOLD TO ROLE FRANCHISEE_USER;
GRANT USAGE ON SCHEMA   RESTAURANT_DB.OPS           TO ROLE FRANCHISEE_USER;
GRANT SELECT ON ALL TABLES IN SCHEMA RESTAURANT_DB.OPS  TO ROLE FRANCHISEE_USER;
