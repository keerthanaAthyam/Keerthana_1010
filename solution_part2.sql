WITH conversion_events AS (
    SELECT 
        user_id,
        branch_id,
        local_user_created_at,
        lead_status,
        first_user_membership_id,
        first_local_membership_purchased_at,
        first_membership_name,
        first_membership_source,
        first_credit_pack_id,
        first_local_credit_pack_purchased_at,
        first_credit_pack_name,
        first_credit_pack_source
    FROM fct_client_conversion_events
)

INSERT INTO fct_lead_conversions (
    user_id,
    branch_id,
    local_user_created_at,
    lead_status,
    client_conversion_event_type,
    client_conversion_event_id,
    client_conversion_event_local_created_at,
    client_conversion_event_name,
    client_conversion_event_source,
    client_conversion_event_filter
)
-- MEMBERSHIP-only records
SELECT 
    user_id,
    branch_id,
    local_user_created_at,
    lead_status,
    'MEMBERSHIP' AS client_conversion_event_type,
    first_user_membership_id AS client_conversion_event_id,
    first_local_membership_purchased_at AS client_conversion_event_local_created_at,
    first_membership_name AS client_conversion_event_name,
    first_membership_source AS client_conversion_event_source,
    'MEMBERSHIP' AS client_conversion_event_filter
FROM conversion_events
WHERE first_user_membership_id IS NOT NULL

UNION ALL

-- CREDIT-only records
SELECT 
    user_id,
    branch_id,
    local_user_created_at,
    lead_status,
    'USER_CREDIT' AS client_conversion_event_type,
    first_credit_pack_id AS client_conversion_event_id,
    first_local_credit_pack_purchased_at AS client_conversion_event_local_created_at,
    first_credit_pack_name AS client_conversion_event_name,
    first_credit_pack_source AS client_conversion_event_source,
    'USER_CREDIT' AS client_conversion_event_filter
FROM conversion_events
WHERE first_credit_pack_id IS NOT NULL

UNION ALL

-- ALL records (both membership and credit)
SELECT 
    user_id,
    branch_id,
    local_user_created_at,
    lead_status,
    CASE 
        WHEN first_local_membership_purchased_at <= first_local_credit_pack_purchased_at OR first_credit_pack_id IS NULL 
        THEN 'MEMBERSHIP' 
        ELSE 'USER_CREDIT' 
    END AS client_conversion_event_type,
    COALESCE(
        CASE 
            WHEN first_local_membership_purchased_at <= first_local_credit_pack_purchased_at OR first_credit_pack_id IS NULL 
            THEN first_user_membership_id 
            ELSE first_credit_pack_id 
        END,
        first_user_membership_id,
        first_credit_pack_id
    ) AS client_conversion_event_id,
    COALESCE(
        CASE 
            WHEN first_local_membership_purchased_at <= first_local_credit_pack_purchased_at OR first_credit_pack_id IS NULL 
            THEN first_local_membership_purchased_at 
            ELSE first_local_credit_pack_purchased_at 
        END,
        first_local_membership_purchased_at,
        first_local_credit_pack_purchased_at
    ) AS client_conversion_event_local_created_at,
    COALESCE(
        CASE 
            WHEN first_local_membership_purchased_at <= first_local_credit_pack_purchased_at OR first_credit_pack_id IS NULL 
            THEN first_membership_name 
            ELSE first_credit_pack_name 
        END,
        first_membership_name,
        first_credit_pack_name
    ) AS client_conversion_event_name,
    COALESCE(
        CASE 
            WHEN first_local_membership_purchased_at <= first_local_credit_pack_purchased_at OR first_credit_pack_id IS NULL 
            THEN first_membership_source 
            ELSE first_credit_pack_source 
        END,
        first_membership_source,
        first_credit_pack_source
    ) AS client_conversion_event_source,
    'ALL' AS client_conversion_event_filter
FROM conversion_events
WHERE first_user_membership_id IS NOT NULL OR first_credit_pack_id IS NOT NULL;