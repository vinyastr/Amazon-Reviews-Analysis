reviews = LOAD '/Users/vinyaskaushiktr/Downloads/amazonreviewsdatasets/sentimentAnalysisdata/Indata1/part-r-00000' AS(marketplace,customer_id,review_id,product_id,product_parent,product_title,product_category,star_rating,helpful_votes,total_votes,vine,verified_purchase,review_headline,review_body,review_date);

sadata = LOAD '/Users/vinyaskaushiktr/Downloads/amazonreviewsdatasets/sentimentAnalysisdata/out/part-r-00000' ;

grpd = GROUP reviews BY (product_id, product_category) ;

result = FOREACH grpd GENERATE group,group.product_category;

joined = JOIN sadata BY $0 LEFT OUTER, result BY group.product_id;

finalResult = FOREACH joined GENERATE $0,$1,$3;

Sorted = ORDER finalResult BY $1 DESC;

STORE Sorted INTO '/Users/vinyaskaushiktr/Downloads/amazonreviewsdatasets/sentimentAnalysisdata/withcategory';