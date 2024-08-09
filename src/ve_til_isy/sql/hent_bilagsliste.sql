-- Denne queryen henter en liste over bilagsnummer som skal behandles.

select distinct
sk_okonomi.dbo.u_linbilag.u_selskap,
sk_okonomi.dbo.u_linbilag.u_aar,
sk_okonomi.dbo.u_linbilag.u_bilag


from sk_okonomi.dbo.u_linbilag

left join sk_okonomi.dbo.u_konterin
on sk_okonomi.dbo.u_linbilag.u_ksid = sk_okonomi.dbo.u_konterin.u_ksid

left join sk_okonomi.dbo.u_bilaghed
on sk_okonomi.dbo.u_linbilag.u_selskap = sk_okonomi.dbo.u_bilaghed.u_selskap
and sk_okonomi.dbo.u_linbilag.u_aar = sk_okonomi.dbo.u_bilaghed.u_aar
and sk_okonomi.dbo.u_linbilag.u_bilag = sk_okonomi.dbo.u_bilaghed.u_bilag

where sk_okonomi.dbo.u_linbilag.u_selskap={u_selskap}
and sk_okonomi.dbo.u_linbilag.u_aar={u_aar}
and sk_okonomi.dbo.u_bilaghed.u_journal>{u_journal}
and sk_okonomi.dbo.u_konterin.u_kb{avdeling_dim} in ({avdeling_filter})
and sk_okonomi.dbo.u_konterin.u_kb{prosjekt_dim} <> ''

order by
sk_okonomi.dbo.u_linbilag.u_selskap,
sk_okonomi.dbo.u_linbilag.u_aar,
sk_okonomi.dbo.u_linbilag.u_bilag
