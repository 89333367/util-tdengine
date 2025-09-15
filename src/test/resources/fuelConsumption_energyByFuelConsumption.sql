select `day`,`fuelConsumption`,`fuelConsumption`*10.7*0.4 `energyByFuelConsumption` from (
select
  `day`,
  sum(
    cast(
		case when `diff_totalFuel` > 0 and `diff_totalFuel` <= 10 then `diff_totalFuel` else 0 end
	  as int)
  ) `fuelConsumption`
from
  (
    select
      to_char (_rowts, 'yyyy-mm-dd') `day`,
      diff (`totalFuel`) `diff_totalFuel`
    from nrvp.v_c
    where `vehicleId` = '{}' and `totalFuel` > 0 and `engineSpeed` > 0
    order by _rowts
  )
group by `day` order by `day`
)