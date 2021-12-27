-- 1. вывести количество фильмов в каждой категории, отсортировать по убыванию.
select
  c.name               as category_name,
  count(c.category_id) as count
from
  "film"
    left join film_category fc on film.film_id = fc.film_id
    left join category c on fc.category_id = c.category_id
group by
  c.category_id
order by
  count(c.category_id) desc;

-- 2. вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию
select
  a.first_name,
  a.last_name,
  count(a.actor_id) as rentals
from
  "actor" a
    inner join film_actor fa on a.actor_id = fa.actor_id
    inner join inventory i on fa.film_id = i.film_id
    inner join rental r on i.inventory_id = r.inventory_id
group by
  a.actor_id
order by
  count(a.actor_id) desc
limit 10;

-- 3. вывести категорию фильмов, на которую потратили больше всего денег.
select category.name, sum(p.amount) as film_total from film f
inner join film_category on f.film_id = film_category.film_id
inner join category on film_category.category_id = category.category_id
inner join inventory i on f.film_id = i.film_id
inner join rental r on i.inventory_id = r.inventory_id
inner join payment p on r.rental_id = p.rental_id
group by category.category_id
order by film_total desc
limit 1;

-- 4. вывести названия фильмов, которых нет в inventory.
-- Написать запрос без использования оператора IN.
select distinct
  title
from
  "film" f
    left join inventory i on f.film_id = i.film_id
where
  i.inventory_id is null;

-- 5. вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”.
-- Если у нескольких актеров одинаковое кол-во фильмов, вывести всех.
with
  cte_top_children_actors as
    (select
       a.*,
       count(a.actor_id) films_number
     from
       actor a
         inner join film_actor fa on a.actor_id = fa.actor_id
         inner join film f on fa.film_id = f.film_id
         inner join film_category fc on f.film_id = fc.film_id
         inner join category c on fc.category_id = c.category_id
     where
       c.name = 'Children'
     group by
       a.actor_id
     order by count(a.actor_id) desc
    )
select
  ctca.first_name,
  ctca.last_name,
  ctca.films_number
from
  cte_top_children_actors ctca
where
    ctca.films_number in (select distinct
                            ctca1.films_number
                          from
                            cte_top_children_actors ctca1
                          order by ctca1.films_number desc
                          limit 3);

-- 6. вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1).
-- Отсортировать по количеству неактивных клиентов по убыванию.
select
  c.city,
  coalesce(c1.count, 0) as inactive,
  coalesce(c2.count, 0) as active
from
  city c
    left join (
    select
      c2.city_id,
      count(c.customer_id) as count
    from
      customer c
        inner join address a on c.address_id = a.address_id
        inner join city c2 on a.city_id = c2.city_id
    where
      c.active = 0
    group by c2.city_id
  ) c1 on c1.city_id = c.city_id
    left join (
    select
      c2.city_id,
      count(c.customer_id) as count
    from
      customer c
        inner join address a on c.address_id = a.address_id
        inner join city c2 on a.city_id = c2.city_id
    where
      c.active = 1
    group by c2.city_id
  ) c2 on c2.city_id = c.city_id
order by
  c1.count desc NULLS LAST, c2.count desc NULLS LAST;

-- 7. вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах
-- (customer.address_id в этом city), и которые начинаются на букву “a”.
-- То же самое сделать для городов в которых есть символ “-”. Написать все в одном запросе.
select
  c3.name,
  sum(EXTRACT(EPOCH FROM (r.return_date - r.rental_date))) as rental_time
from
  film f
    inner join film_category fc on f.film_id = fc.film_id
    inner join category c3 on fc.category_id = c3.category_id
    inner join inventory i on f.film_id = i.film_id
    inner join rental r on i.inventory_id = r.inventory_id
    inner join customer c on r.customer_id = c.customer_id
    inner join address a on c.address_id = a.address_id
    inner join city c2 on a.city_id = c2.city_id
where c2.city ilike '%-%' and f.title ilike 'a%'
group by
  c3.category_id
order by rental_time desc NULLS LAST
limit 1;