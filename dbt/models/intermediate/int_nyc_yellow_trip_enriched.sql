with staging as (
    select * from {{ ref('stg_nyc_yellow_trip') }}
),
enriched as (
    select
        s.*,
        v.vendor_name,
        p.payment_type_description,
        r.ratecode_description
    from staging s
    left join {{ ref('vendor_info') }} v on v.vendor_id = s.vendor_id
    left join {{ ref('payment_info') }} p on s.payment_type = p.payment_type
    left join {{ ref('ratecode_info') }} r on s.ratecode_id = r.ratecode_id
)
select * from enriched