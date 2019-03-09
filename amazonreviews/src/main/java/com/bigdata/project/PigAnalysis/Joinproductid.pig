reviews = LOAD '/Users/vinyaskaushiktr/Downloads/amazonreviewsdatasets/sentimentAnalysisdata/Indata1/part-r-00000' AS(marketplace,customer_id,review_id,product_id,product_parent,product_title,product_category,star_rating,helpful_votes,total_votes,vine,verified_purchase,review_headline,review_body,review_date);

top5 = LOAD '/Users/vinyaskaushiktr/Downloads/amazonreviewsdatasets/Top25Output/Top25ByYear/2012/part-r-00000' ;

grpd = GROUP reviews BY (product_id, product_category) ;

result = FOREACH grpd GENERATE group,group.product_category;

joined = JOIN top5 BY $0 LEFT OUTER, result BY group.product_id;

finalResult = FOREACH joined GENERATE $0,$1,$2,$3,$5;

Sorted = ORDER finalResult BY $3 ASC;

STORE Sorted INTO '/Users/vinyaskaushiktr/Downloads/amazonreviewsdatasets/Top25Output/Top25ByYearOut/2012/';