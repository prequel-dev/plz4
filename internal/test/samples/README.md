Samples are a subset of the [Silesia Corpus](https://sun.aei.polsl.pl/~sdeor/index.php?page=silesia).





| Name         |  Description | Provenance | Notes | 
| ------------ |  ----------- | ---------- | ----- |
| webster.bz2  | The 1913 Webster Unabridged Dictionary  | [Silesia Corpus](https://sun.aei.polsl.pl/~sdeor/index.php?page=silesia) | raw data
| dickens.lz4 | Collected works of Charles Dickens | [Silesia Corpus](https://sun.aei.polsl.pl/~sdeor/index.php?page=silesia)  | lz4c -B7 -BX --best dickens
| dickens_dict.lz4 | Collected works of Charles Dickens | [Silesia Corpus](https://sun.aei.polsl.pl/~sdeor/index.php?page=silesia)  | lz4c --best --content-size -D ./dickens dickens
| dickens_linked.lz4 | Collected works of Charles Dickens | [Silesia Corpus](https://sun.aei.polsl.pl/~sdeor/index.php?page=silesia)  | lz4c -BD -BX -12 ./dickens
| dickens_linked_dict.lz4 | Collected works of Charles Dickens | [Silesia Corpus](https://sun.aei.polsl.pl/~sdeor/index.php?page=silesia)  | lz4c -12 -BD -BX --content-size -D ./dickens dickens
| mr.lz4 | Medical magnetic resonance image |  [Silesia Corpus](https://sun.aei.polsl.pl/~sdeor/index.php?page=silesia)  | lz4c -B4 --no-frame-crc mr    