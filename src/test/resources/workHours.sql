select
  `day`,
  sum(
	cast(
		case when `diff_engineWorkTime` > 0 and `diff_engineWorkTime` <= 1000 then `diff_engineWorkTime` else 0 end
	as int)
  ) / 3600 `workHours`
from
  (
    select
      to_char (_rowts, 'yyyy-mm-dd') `day`,
      diff (`engineWorkTime`) `diff_engineWorkTime`
    from nrvp.v_c
    where `vehicleId` = '{}' and `engineWorkTime` > 0 and `engineWorkTime` < 10*365*86400
    order by _rowts
  )
group by `day` order by `day`