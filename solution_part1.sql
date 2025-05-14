
WITH 
-- First credit pack purchase per user
first_credit_packs AS (
    SELECT 
        user_id,
        branch_id,
        credit_pack_id AS first_credit_pack_id,
        credit_pack_purchased_at AS first_local_credit_pack_purchased_at,
        credit_pack_purchase_details->>'name' AS first_credit_pack_name,
        credit_pack_purchase_details->>'source' AS first_credit_pack_source,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY credit_pack_purchased_at) AS rn
    FROM fct_credit_pack_purchases
    WHERE credit_pack_purchased_at IS NOT NULL
),

-- First membership purchase per user
first_memberships AS (
    SELECT 
        user_id,
        branch_id,
        membership_id AS first_user_membership_id,
        membership_purchased_at AS first_local_membership_purchased_at,
        membership_purchase_details->>'name' AS first_membership_name,
        membership_purchase_details->>'source' AS first_membership_source,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY membership_purchased_at) AS rn
    FROM fct_membership_purchases
    WHERE membership_purchased_at IS NOT NULL
)

-- Insert into final table
INSERT INTO fct_client_conversion_events (
    user_id,
    branch_id,
    local_user_created_at,
    lead_status,
    client_conversion_event_type,
    client_conversion_event_id,
    client_conversion_event_local_created_at,
    client_conversion_event_name,
    client_conversion_event_source,
    first_user_membership_id,
    first_local_membership_purchased_at,
    first_membership_name,
    first_membership_source,
    first_credit_pack_id,
    first_local_credit_pack_purchased_at,
    first_credit_pack_name,
    first_credit_pack_source
)
SELECT 
    u.user_id,
    u.branch_id,
    u.created_at AS local_user_created_at,
    'CLIENT' AS lead_status,
    CASE 
        WHEN m.first_user_membership_id IS NOT NULL AND c.first_credit_pack_id IS NOT NULL THEN
            CASE 
                WHEN m.first_local_membership_purchased_at <= c.first_local_credit_pack_purchased_at THEN 'MEMBERSHIP'
                ELSE 'USER_CREDIT'
            END
        WHEN m.first_user_membership_id IS NOT NULL THEN 'MEMBERSHIP'
        WHEN c.first_credit_pack_id IS NOT NULL THEN 'USER_CREDIT'
    END AS client_conversion_event_type,
    COALESCE(
        CASE 
            WHEN m.first_local_membership_purchased_at <= c.first_local_credit_pack_purchased_at OR c.first_credit_pack_id IS NULL 
            THEN m.first_user_membership_id 
            ELSE c.first_credit_pack_id 
        END,
        m.first_user_membership_id,
        c.first_credit_pack_id
    ) AS client_conversion_event_id,
    COALESCE(
        CASE 
            WHEN m.first_local_membership_purchased_at <= c.first_local_credit_pack_purchased_at OR c.first_credit_pack_id IS NULL 
            THEN m.first_local_membership_purchased_at 
            ELSE c.first_local_credit_pack_purchased_at 
        END,
        m.first_local_membership_purchased_at,
        c.first_local_credit_pack_purchased_at
    ) AS client_conversion_event_local_created_at,
    COALESCE(
        CASE 
            WHEN m.first_local_membership_purchased_at <= c.first_local_credit_pack_purchased_at OR c.first_credit_pack_id IS NULL 
            THEN m.first_membership_name 
            ELSE c.first_credit_pack_name 
        END,
        m.first_membership_name,
        c.first_credit_pack_name
    ) AS client_conversion_event_name,
    COALESCE(
        CASE 
            WHEN m.first_local_membership_purchased_at <= c.first_local_credit_pack_purchased_at OR c.first_credit_pack_id IS NULL 
            THEN m.first_membership_source 
            ELSE c.first_credit_pack_source 
        END,
        m.first_membership_source,
        c.first_credit_pack_source
    ) AS client_conversion_event_source,
    m.first_user_membership_id,
    m.first_local_membership_purchased_at,
    m.first_membership_name,
    m.first_membership_source,
    c.first_credit_pack_id,
    c.first_local_credit_pack_purchased_at,
    c.first_credit_pack_name,
    c.first_credit_pack_source
FROM dim_user u
LEFT JOIN first_memberships m ON u.user_id = m.user_id AND m.rn = 1
LEFT JOIN first_credit_packs c ON u.user_id = c.user_id AND c.rn = 1
WHERE m.first_user_membership_id IS NOT NULL OR c.first_credit_pack_id IS NOT NULL;