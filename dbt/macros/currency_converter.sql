{% macro convert_to_usd(amount_col, currency_col) %}
    {#
        Converts given monetary amount into USD based on currency code.
        Supports standard e-commerce multi-currency transactions.
    #}
    case 
        when upper({{ currency_col }}) = 'USD' then round(cast({{ amount_col }} as numeric), 2)
        when upper({{ currency_col }}) = 'EUR' then round(cast({{ amount_col }} as numeric) * 1.08, 2)
        when upper({{ currency_col }}) = 'GBP' then round(cast({{ amount_col }} as numeric) * 1.27, 2)
        when upper({{ currency_col }}) = 'CAD' then round(cast({{ amount_col }} as numeric) * 0.74, 2)
        when upper({{ currency_col }}) = 'AUD' then round(cast({{ amount_col }} as numeric) * 0.66, 2)
        else round(cast({{ amount_col }} as numeric), 2)
    end
{% endmacro %}
