-- Workbook Metadata Stats

select
    s.name as site_name,
    w.project_name,
    w.name,
    w.workbook_url,
    ds1.total_datasources,
    ds2.redshift_datasources,
    ds3.cda_redshift_datasources,
    w.owner_name,
    su.email,
    ds4.views,
    ds4.sheets,
    w.created_at,
    w.updated_at
from _workbooks w
left outer join sites s on w.site_id = s.id
left outer join
    (
         select d.parent_workbook_id,
                count(d.id) total_datasources
         from datasources d
                  left outer join data_connections dc
                                  on d.id = dc.datasource_id
         group by d.parent_workbook_id

     ) ds1 on w.id = ds1.parent_workbook_id

left outer join
     (
         select d.parent_workbook_id,
                count(d.id) redshift_datasources
         from datasources d
                  left outer join data_connections dc
                                  on d.id = dc.datasource_id
         where dc.dbclass = 'redshift'
         group by d.parent_workbook_id
     ) ds2 on w.id = ds2.parent_workbook_id

left outer join
     (
         select d.parent_workbook_id,
                count(d.id) cda_redshift_datasources
         from datasources d
                  left outer join data_connections dc
                                  on d.id = dc.datasource_id
         where dc.dbclass = 'redshift'
         and dc.server in ('business-intelligence-data-warehouse.weworkers.io','redshift-production.weworkers.io')
         group by d.parent_workbook_id
     ) ds3 on w.id = ds3.parent_workbook_id

left outer join

(
    select wb.id, sum(nviews) as views, count(distinct v.id) sheets
    from views_stats vs
    join "views" v on v.id = vs.view_id
    join workbooks wb on wb.id = v.workbook_id
    where vs.time >= '2019-01-01'
    and wb.site_id = 3
    group by wb.id
) ds4 on w.id = ds4.id

left outer join system_users su on lower(su.name) = lower(w.owner_name)

where w.site_id = 3; -- Data & Analytics Site
