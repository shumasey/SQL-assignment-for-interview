# Тестовое задание для собеседования
## Задача
Для каждого пользователя вычислить его любимую зону (любимой называем ту зону, в которой он больше всего простоял за интервал времни к примеру январь - февраль 2021 года и кол-во времени должно превышать 5 часов за 2 месяца
Далее так же для каждой любимой пользователь-любимая зона надо понять регулярно или нет он там останавливался. Регулярно будем считать ту зону в которой кол-во уникальных дней за январь-февраль более 15.
И дальше по произвольному дню нужно:
для каждого часа-зоны- сумма времени регуляршиков, сумма всех времен, доля регуляршиков.

Описание структуры таблицы:
ID - идентификатор сессии
ACCOUNTID - идентификатор пользователя
START_TIME - время старта сессии в unixtime
END_TIME - время окончания сессии в unixtime
ZONENUMBER - идентификатор парковочной зоны

Сама выгрузка в csv https://disk.yandex.ru/d/wWGXEvE-t0b_kQ

## Решение
```SQL
with res as (
	with lzreg as (
--сначала вычисляем регулярщиков в любимых зонах
		select accountid, min(zonenumber) zonenumber, max(srok) 
		from 
			(select accountid, zonenumber, sum(extract(epoch from to_timestamp(end_time/1000))/3600-
				extract(epoch from to_timestamp(start_time/1000))/3600) as srok,
				count(distinct(floor(extract(epoch from to_timestamp(start_time/1000))/(3600*24)))) as days 
			from public.data d 
			where to_timestamp(start_time/1000) between '2021-01-01' and '2022-03-01'
			group by accountid, zonenumber
			having sum(extract(epoch from to_timestamp(end_time/1000))/3600-
				extract(epoch from to_timestamp(start_time/1000))/3600)>5 and
				count(distinct(floor(extract(epoch from to_timestamp(start_time/1000))/(3600*24))))>15 
			order by 1, 3 desc) likezone 
		group by 1)
--далее используется внешний произвольный параметр типа $P{SOME_DAY}
--если формат SOME_DAY такой же, как в базе, то вычисление суммарного времени регулярщиков по часу-зоне будет выглядеть так
	select chas, lzreg.zonenumber, lzreg.accountid,
		sum(case when date_trunc('hour',to_timestamp(start_time/1000))=chas
				and date_trunc('hour',to_timestamp(end_time/1000))=chas then 
				extract(epoch from to_timestamp(end_time/1000))/3600-extract(epoch from to_timestamp(start_time/1000))/3600
			when date_trunc('hour',to_timestamp(start_time/1000))=chas
				and date_trunc('hour',to_timestamp(end_time/1000))!=chas then 
				ceil(extract(epoch from to_timestamp(start_time/1000))/3600)-extract(epoch from to_timestamp(start_time/1000))/3600
			when date_trunc('hour',to_timestamp(start_time/1000))!=chas
				and date_trunc('hour',to_timestamp(end_time/1000))=chas then 
				extract(epoch from to_timestamp(end_time/1000))/3600-floor(extract(epoch from to_timestamp(start_time/1000))/3600)
			when date_trunc('hour',to_timestamp(start_time/1000))<chas
				and date_trunc('hour',to_timestamp(end_time/1000))>chas then 1 else 0 end) vremya
	from lzreg 
	join public.data on (lzreg.zonenumber,lzreg.accountid)=(data.zonenumber,data.accountid)
	cross join (select * from generate_series($P{SOME_DAY}::timestamp,$P{SOME_DAY}::timestamp+'1 day'::interval,'1 hour'::interval) chas) chasy
	where to_timestamp(start_time/1000)>$P{SOME_DAY} and to_timestamp(end_time/1000)<$P{SOME_DAY}::timestamp+'1 day'::interval
	group by 1, 2, rollup(3))
--и, наконец, вычисляем доли регулярщиков
select chas, zonenumber, case when accountid is null then 'Итого' else accountid::text end accountid,
	vremya, vremya/(sum(vremya) over(partition by chas, zonenumber))*2 dolya
from res 
where vremya>0
```

# Еще одно задание
## Задача
У вас SQL база с таблицами:
1) Users(userId, age)
2) Purchases (purchaseId, userId, itemId, date)
3) Items (itemId, price).
Напишите SQL запросы для расчета следующих метрик:

А) какую сумму в среднем в месяц тратит:
- пользователи в возрастном диапазоне от 18 до 25 лет включительно
- пользователи в возрастном диапазоне от 26 до 35 лет включительно

Б) в каком месяце года выручка от пользователей в возрастном диапазоне 35+ самая большая

В) какой товар обеспечивает дает наибольший вклад в выручку за последний год

Г) топ-3 товаров по выручке и их доля в общей выручке за любой год

## Решение
```SQL
--A)
with resul as (select sum(case when Users.age between 18 and 25 then Items.price else 0 end) firstsum,
					sum(case when Users.age between 26 and 35 then Items.price else 0 end) secondsum,
					count(distinct date_trunc('month',Purchses.date)) totalmonth
				from Purchases
				join Users using(UserId)
				join Items using(ItemId))
select firstsum/totalmonth firstavg, secondsum/totalmonth secondavg 
from resul

--Б)
with resul as (select date_part('year',Purchses.date) god, date_trunc('month',Purchses.date) mes, sum(Items.price) itog,
				rank() over(partition by date_part('year',Purchses.date) order by sum(Items.price) desc) rang
				from Purchases
				join Users using(UserId)
				join Items using(ItemId)
				where Users.age >=35
				group by 1, 2)
select god, mes, itog 
from resul 
where rang=1 
order by god

--В)
select Purchses.ItemId tovar, sum(Items.price) itog,
	rank() over(partition by Purchses.ItemId order by sum(Items.price) desc) rang
from Purchases
join Items using(ItemId)
where date_part('year',Purchses.date)=(select max(date_part('year',Purchses.date)) from Purchases)
group by 1 
order by 3 
limit 1

--Г) Если вы хотите получить результаты за каждый год
with resul as (select date_part('year',Purchses.date) god, Purchses.ItemId tovar, sum(Items.price) itog,
				sum(Items.price)/(sum(sum(Items.price)) over (partition by date_part('year',Purchses.date))) dolya,
				rank() over(partition by Purchses.ItemId order by sum(Items.price) desc) rang
				from Purchases
				join Items using(ItemId)
				group by 1, 2) 
select god, tovar, itog, dolya
from resul
where rang in (1,2,3)
order by god, itog desc

--Г) Если год вводится извне в виде параметра $P{YEAR}
with resul as (select Purchses.ItemId tovar, sum(Items.price) itog,
				sum(Items.price)/(sum(sum(Items.price)) over ()) dolya,
				rank() over(partition by Purchses.ItemId order by sum(Items.price) desc) rang
				from Purchases
				join Items using(ItemId)
				where date_part('year',Purchses.date)=$P{YEAR}
				group by 1) 
select god, tovar, itog, dolya
from resul
where rang in (1,2,3)
order by god, itog desc
```