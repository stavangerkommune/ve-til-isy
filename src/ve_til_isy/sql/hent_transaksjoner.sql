-- Definerer CTE for å forenkle JOIN-operasjonene
WITH BilagInfo AS (
  SELECT
    lb.u_selskap AS selskap,
    lb.u_aar AS aar,
    lb.u_periode AS periode,
    bh.u_journal AS journalnr,
  	bh.u_btype AS bilagstype,
    lb.u_bilag AS bilag,
    lb.u_bilaglin AS bilaglin,
    lb.u_blinje AS blinje,
    kt.u_kb02 AS ansvar,
    kt.u_kb04 AS prosjekt,
    kt.u_kb09 AS dispnr,
    kt.u_kb10 AS etr,
    lb.u_mvakode AS mvakode,
    lb.u_resknr AS resknr,
    lb.u_faktlop AS faktlop,
    lb.u_tekst AS tekst,
    tr.u_fodtnr AS orgnr,
    tr.u_navn AS navn,
    fa.u_faktnr AS faktnr,
    fa.u_faktdato AS faktdato,
    fa.u_forfdato AS forfdato,
    lb.u_nokbelop AS netto
  FROM
    sk_okonomi.dbo.u_linbilag AS lb
  LEFT JOIN sk_okonomi.dbo.u_konterin AS kt ON lb.u_ksid = kt.u_ksid
  LEFT JOIN sk_okonomi.dbo.u_bilaghed AS bh ON lb.u_selskap = bh.u_selskap
    AND lb.u_aar = bh.u_aar
    AND lb.u_bilag = bh.u_bilag
  LEFT JOIN sk_okonomi.dbo.u_faktura AS fa ON lb.u_selskap = fa.u_selskap
    AND lb.u_aar = fa.u_aar
    AND lb.u_faktlop = fa.u_faktlop
  LEFT JOIN sk_okonomi.dbo.u_typeresk AS tr ON fa.u_selskap = tr.u_selskap -- Må joine på u_faktura, for å kunne filtrere på resktype.
    AND fa.u_resknr = tr.u_resknr
	AND fa.u_resktype = tr.u_resktype
)
-- Hovedspørring som bruker CTE
SELECT
  *
FROM
  BilagInfo
WHERE
  Selskap = {u_selskap}
  AND Aar = {u_aar}
  AND Bilag IN ({bilagsliste})
ORDER BY
  Journalnr,
  Bilag,
  Blinje