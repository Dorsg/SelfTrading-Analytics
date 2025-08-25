
/** Format ISO/epoch strings into “Jan 05 2025, 14:07” (locale-aware) */
export function formatPrettyDate (value) {
  if (!value) return '';
  const d = new Date(value);
  return d.toLocaleString(undefined, {
    year  : 'numeric',
    month : 'short',
    day   : '2-digit',
    hour  : '2-digit',
    minute: '2-digit',
  });
}

/** `$ 12,345.67` with two decimals, locale-aware */
export function formatCurrency (num) {
  return `$ ${Number(num ?? 0).toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`;
}
