from collections import namedtuple, Counter
from pathlib import Path

pool = namedtuple("Pool", ["chain", "pair", "exchange", "address", "token0", "token1", "to_process"])

REFRESH_EVERY = 600  # In seconds
# REFRESH_EVERY = 10  # In seconds
SAVE_TO_FOLDER = Path(r"./data")
FILENAME = "liquidities_history.csv"

# Change key here.
API_KEY = "ckey_XXXXXXXXXXXXXX"

COVALENT_ENDPOINT = "https://api.covalenthq.com/v1/"

ETHEREUM_NET_ID = 1
MATIC_NET_ID = 137
AVALANCHE_NET_ID = 43114
MOONRIVER_NET_ID = 1285
ARBITRUM_NET_ID = 42161
FANTOM_NET_ID = 250

asset_addresses = {
    "OHMv1 - Ethereum": "0x383518188c0c6d7730d91b2c03a03c837814a899"
    , "OHM - Ethereum": "0x64aa3364F17a4D01c6f1751Fd97C2BD3D7e7f1D5"
    , "gOHM - Ethereum": "0x0ab87046fBb341D058F17CBC4c1133F25a20a52f"
    , "sOHMv1 - Ethereum": "0x04f2694c8fcee23e8fd0dfea1d4f5bb8c352111f"
    , "sOHM - Ethereum": "0x04906695D6D12CF5459975d7C3C03356E4Ccd460"
    , "wsOHM - Ethereum": "0xca76543cf381ebbb277be79574059e32108e3e65"

    , "wsOHM - Arbitrum": "0x739ca6d71365a08f584c8fc4e1029045fa8abc4b"
    , "gOHM - Arbitrum": "0x8D9bA570D6cb60C7e3e0F31343Efe75AB8E65FB1"

    , "wsOHM - Avalanche": "0x8CD309e14575203535EF120b5b0Ab4DDeD0C2073"
    , "gOHM - Avalanche": "0x321e7092a180bb43555132ec53aaa65a5bf84251"

    , "gOHM - Fantom": "0x91fa20244Fb509e8289CA630E5db3E9166233FDc"

    , "gOHM - Polygon": "0xd8cA34fd379d9ca3C6Ee3b3905678320F5b45195"

    , "gOHM - Moonriver": "0x3bF21Ce864e58731B6f28D68d5928BcBEb0Ad172"


}

rewarder_addresses = {
    "Avalanche - 0xb674f93952f02f2538214d4572aa47f262e990ff": "0xe65c29f1c40b52cf3a601a60df6ad37c59af1261"
    , "Arbitrum - 0xaa5bd49f2162ffdc15634c87a77ac67bd51c6a6d": "0xAE961A7D116bFD9B2534ad27fE4d178Ed188C87A"
    , "Polygon - 0x1549e0e8127d380080aab448b82d280433ce4030": "0x71581bf0ce397f50f87cc2490146d30a1e686461"
    , "Avalanche - 0x3e6be71de004363379d864006aac37c9f55f8329": "0x99ad2a9a0d4a15d861c0b60c7df8965d1b3e18d8"
}

chain_dict = {
    "Ethereum": ETHEREUM_NET_ID
    , "Polygon": 137
    , "Avalanche": 43114
    , "Moonriver": 1285
    , "Arbitrum": 42161
    , "Fantom": 250
    , "Binance Smart Chain": 56
}

pools = [
    pool("Arbitrum", "wsOHM-WETH", "Sushiswap", "0x80840c4592e31fad23b09fb3af39d9d2b48da00c", "0x739ca6d71365a08f584c8fc4e1029045fa8abc4b", "0x82af49447d8a07e3bd95bd0d56f35241523fbab1", True)
    , pool("Arbitrum", "wsOHM-USDT", "Sushiswap", "0x976e80313a441274d8cebf419fc90ad0372f8424", "0x739ca6d71365a08f584c8fc4e1029045fa8abc4b", "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9", True)
    , pool("Arbitrum", "wsOHM-SUSHI", "Sushiswap", "0xaf04180d18a969df3aeb7fc3e452c5c55d35bce0", "0x739ca6d71365a08f584c8fc4e1029045fa8abc4b", "0xd4d42f0b6def4ce0383636770ef773390d85c61a", True)
    , pool("Arbitrum", "wsOHM-USDC", "Sushiswap", "0x6756351f8586bc01cdc36b9300b3e9076bc8881f", "0x739ca6d71365a08f584c8fc4e1029045fa8abc4b", "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8", True)
    , pool("Arbitrum", "wsOHM-MIM", "Sushiswap", "0xe1865aebc305a9f42a05e909fa26f3f2bb0c6525", "0x739ca6d71365a08f584c8fc4e1029045fa8abc4b", "0xfea7a6a0b346362bf88a9e4a88416b77a57d6c2a", True)
    , pool("Arbitrum", "WETH-gOHM", "Sushiswap", "0xaa5bd49f2162ffdc15634c87a77ac67bd51c6a6d", "0x82af49447d8a07e3bd95bd0d56f35241523fbab1", "0x8d9ba570d6cb60c7e3e0f31343efe75ab8e65fb1", True)
    , pool("Arbitrum", "wsOHM-gOHM", "Sushiswap", "0x30ca99fb2ba42072713bd188bfbf4029f6ac31db", "0x739ca6d71365a08f584c8fc4e1029045fa8abc4b", "0x8d9ba570d6cb60c7e3e0f31343efe75ab8e65fb1", True)
    , pool("Arbitrum", "MAGIC-gOHM", "Sushiswap", "0xac75a1a0c4933e6537eafb6af3d402f82a459389", "0x539bde0d7dbd336b79148aa742883198bbf60342", "0x8d9ba570d6cb60c7e3e0f31343efe75ab8e65fb1", True)
    , pool("Arbitrum", "gOHM-SUSHI", "Sushiswap", "0xcc51011b2b2da03b6f8b303ba496db173a97bc8d", "0x8d9ba570d6cb60c7e3e0f31343efe75ab8e65fb1", "0xd4d42f0b6def4ce0383636770ef773390d85c61a", True)

    , pool("Avalanche", "MIM-wsOHM", "TraderJoe", "0xaa92c71bb5ecb6a7311e4239b93ad8ae173e0c05", "0x130966628846BFd36ff31a822705796e8cb8C18D", "0x8cd309e14575203535ef120b5b0ab4dded0c2073", True)
    , pool("Avalanche", "wsOHM-USDC.e", "TraderJoe", "0xb16624176fb1c5191094743ccf13b21775f18c7c", "0x8cd309e14575203535ef120b5b0ab4dded0c2073", "0xa7d7079b0fead91f3e65f86e8915cb59c1a4c664", True)
    , pool("Avalanche", "WBTC.e-wsOHM", "TraderJoe", "0x1165c5739b387ac9457fcea19afd65b4b4b3b066", "0x50b7545627a5162f82a992c33b87adc75187b218", "0x8cd309e14575203535ef120b5b0ab4dded0c2073", True)
    , pool("Avalanche", "wsOHM-WAVAX", "TraderJoe", "0x95efcc8e728c3532ade74629603939e4146600f6", "0x8cd309e14575203535ef120b5b0ab4dded0c2073", "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7", True)
    , pool("Avalanche", "wsOHM-renBTC", "TraderJoe", "0xc59e48dae42f6e3b45d0c6072cb0a6c870a6debb", "0x8cd309e14575203535ef120b5b0ab4dded0c2073", "0xdbf31df14b66535af65aac99c32e9ea844e14501", True)
    , pool("Avalanche", "xJOE-wsOHM", "TraderJoe", "0x69ece6fe13b64e57b14df1ee9a2259bdd95e7633", "0x57319d41f71e81f3c65f2a47ca4e001ebafd4f33", "0x8cd309e14575203535ef120b5b0ab4dded0c2073", True)
    , pool("Avalanche", "DAV-wsOHM", "TraderJoe", "0x42d1cdd9b375be46cebcb0bd551030c61ee4cbce", "0x70514d6e5ff45aecb8a95a9e0176eaba4465938b", "0x8cd309e14575203535ef120b5b0ab4dded0c2073", True)
    , pool("Avalanche", "OTWO-wsOHM", "TraderJoe", "0xc54c28a4898a11f7f510dead677fe7d9fd014939", "0xaa2439dbad718c9329a5893a51a708c015f76346", "0x8cd309e14575203535ef120b5b0ab4dded0c2073", True)
    , pool("Avalanche", "wsOHM-CRACK", "TraderJoe", "0x88b1aea2ee541d9c39ca690f8b376569f49c1ddb", "0x8cd309e14575203535ef120b5b0ab4dded0c2073", "0xe9d00cbc5f02614d7281d742e6e815a47ce31107", True)
    , pool("Avalanche", "wsOHM-SMRT", "TraderJoe", "0x604ebac4bf68887443611a3776dddee08ea728fe", "0x8cd309e14575203535ef120b5b0ab4dded0c2073", "0xcc2f1d827b18321254223df4e84de399d9ff116c", True)
    , pool("Avalanche", "wsOHM-SMRT", "TraderJoe", "0xd26380ab4c5ad67a301e64ede4380b3445a2bfb7", "0x8cd309e14575203535ef120b5b0ab4dded0c2073", "0xcc2f1d827b18321254223df4e84de399d9ff116c", True)
    , pool("Avalanche", "wsOHM-TIME", "TraderJoe", "0x9de5526d1eb61d53b2d54c917bf4672984477dc5", "0x8cd309e14575203535ef120b5b0ab4dded0c2073", "0xb54f16fb19478766a268f172c9480f8da1a7c9c3", True)
    , pool("Avalanche", "wsOHM-OTWO", "TraderJoe", "0x918028f9bb2c7a707e2f194a82000923dd7338f3", "0x8cd309e14575203535ef120b5b0ab4dded0c2073", "0xaa2439dbad718c9329a5893a51a708c015f76346", True)
    , pool("Avalanche", "gOHM-WAVAX", "TraderJoe", "0xb674f93952f02f2538214d4572aa47f262e990ff", "0x321e7092a180bb43555132ec53aaa65a5bf84251", "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7", True)
    , pool("Avalanche", "gOHM-wsOHM", "TraderJoe", "0x5d577c817bd4003a9b794c33ef45d0d6d4138bea", "0x321e7092a180bb43555132ec53aaa65a5bf84251", "0x8cd309e14575203535ef120b5b0ab4dded0c2073", True)
    , pool("Avalanche", "wsOHM-tt2", "TraderJoe", "0xbb087ede665ece0820ae3f204e8d32c9d1b9c03c", "0x8cd309e14575203535ef120b5b0ab4dded0c2073", "0xe993f2a0a9133331b2e85357b0e545c29587bc03", True)
    , pool("Avalanche", "MEMO-wsOHM", "TraderJoe", "0x6a3c3007a0f41edc209b069c4a53a7be3c449a04", "0x136acd46c134e8269052c62a67042d6bdedde3c9", "0x8cd309e14575203535ef120b5b0ab4dded0c2073", True)
    , pool("Avalanche", "wMEMO-wsOHM", "TraderJoe", "0x497fbe7b5418b0f3b93cd86b448afc8dbb7933b5", "0x0da67235dd5787d67955420c84ca1cecd4e5bb3b", "0x8cd309e14575203535ef120b5b0ab4dded0c2073", True)
    , pool("Avalanche", "LINK.e-wsOHM", "TraderJoe", "0xf2cde4eaf81bc06843d423e157c73218320a74d8", "0x5947bb275c521040051d82396192181b413227a3", "0x8cd309e14575203535ef120b5b0ab4dded0c2073", True)
    , pool("Avalanche", "SMRTr-wsOHM", "TraderJoe", "0x8b8d118fd6aa806f9f8da39374e2fd0f081e19da", "0x6d923f688c7ff287dc3a5943caeefc994f97b290", "0x8cd309e14575203535ef120b5b0ab4dded0c2073", True)
    , pool("Avalanche", "OTWO-wsOHM", "TraderJoe", "0xcb773d624c412f7f8f30145648db96cf515bb5fe", "0xaa2439dbad718c9329a5893a51a708c015f76346", "0x8cd309e14575203535ef120b5b0ab4dded0c2073", True)
    , pool("Avalanche", "gOHM-FETCH", "TraderJoe", "0xf60624661645946c4c2f7514f632fe16ce1cb400", "0x321e7092a180bb43555132ec53aaa65a5bf84251", "0xc1986af1492c9b091b1bb30e700536ac5a52761a", True)
    , pool("Avalanche", "gOHM-SNOB", "TraderJoe", "0xae94637eb12ecdb47529de0b8c754b11039aa6b7", "0x321e7092a180bb43555132ec53aaa65a5bf84251", "0xc38f41a296a4493ff429f1238e030924a1542e50", True)
    , pool("Avalanche", "gOHM-FETCH", "TraderJoe", "0xf3914556c81b330cfb2d4ff90eb02981a6a7a335", "0x321e7092a180bb43555132ec53aaa65a5bf84251", "0x5ca50394f77d7ccf7922d519dd699ea75f4220b8", True)
    , pool("Avalanche", "gOHM-USDT.e", "TraderJoe", "0x26920501e6d46606327d0c5e6e97559cbb594daf", "0x321e7092a180bb43555132ec53aaa65a5bf84251", "0xc7198437980c041c805a1edcba50c1ce5db95118", True)
    , pool("Avalanche", "gOHM-OTWO", "TraderJoe", "0x7bc2561d69b56fae9760df394a9fa9202c5f1f11", "0x321e7092a180bb43555132ec53aaa65a5bf84251", "0xaa2439dbad718c9329a5893a51a708c015f76346", True)
    , pool("Avalanche", "gOHM-JOE", "TraderJoe", "0x4888a4220bc3d70ec8fc37eab0e68ce59d8eb6d3", "0x321e7092a180bb43555132ec53aaa65a5bf84251", "0x6e84a6216ea6dacc71ee8e6b0a5b7322eebc0fdd", True)
    , pool("Avalanche", "gOHM-SB", "TraderJoe", "0xc111b8fe7f8868107ab677c7fc762eb51115e2b9", "0x321e7092a180bb43555132ec53aaa65a5bf84251", "0x7d1232b90d3f809a54eeaeebc639c62df8a8942f", True)
    , pool("Avalanche", "gOHM-WAVAX", "TraderJoe", "0xb68f4e8261a4276336698f5b11dc46396cf07a22", "0x321e7092a180bb43555132ec53aaa65a5bf84251", "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7", True)
    , pool("Avalanche", "gOHM-Gnome", "TraderJoe", "0xb544e7b4c775e641b4ae4efc43dc5337c8228af4", "0x321e7092a180bb43555132ec53aaa65a5bf84251", "0x967d14dc31ab02eddf508f921eb35d02b4674234", True)
    , pool("Avalanche", "gOHM-USDC.e", "TraderJoe", "0x3e61ff7487f0ca92b4f5bec8fe540a8c7caad28b", "0x321e7092a180bb43555132ec53aaa65a5bf84251", "0xa7d7079b0fead91f3e65f86e8915cb59c1a4c664", True)
    , pool("Avalanche", "MIM-gOHM", "TraderJoe", "0xf0573e7353c64c66aefb3c839797b0ca05443898", "0x130966628846bfd36ff31a822705796e8cb8c18d", "0x321e7092a180bb43555132ec53aaa65a5bf84251", True)
    , pool("Avalanche", "gOHM-FRAX", "TraderJoe", "0x3e6be71de004363379d864006aac37c9f55f8329", "0x321e7092a180bb43555132ec53aaa65a5bf84251", "0xd24c2ad096400b6fbcd2ad8b24e7acbc21a1da64", True)
    , pool("Avalanche", "gOHM-WETH.e", "TraderJoe", "0x23a4cae839b84443e4457ac15dbb8c61a13f1c92", "0x321e7092a180bb43555132ec53aaa65a5bf84251", "0x49d5c2bdffac6ce2bfdb6640f4f80f226bc10bab", True)

    , pool("Avalanche", "gOHM-INDEX", "Pangolin", "0xd7babf820ba35e9dfb59b830292417ec53fbd8c1", "0x321e7092a180bb43555132ec53aaa65a5bf84251", "0x56cb0452ee767f8c4036587613ccc84d168534cd", True)
    , pool("Avalanche", "gOHM-INDEX", "TraderJoe", "0x04aa15b99f62a5811a89c585dfa851c2c43764f8", "0x321e7092a180bb43555132ec53aaa65a5bf84251", "0x56cb0452ee767f8c4036587613ccc84d168534cd", True)

    , pool("Avalanche", "gOHM-WAVAX", "Sushiswap", "0xf642f80655c63f687eaf12838ceaf2909d31ef52", "0x321e7092a180bb43555132ec53aaa65a5bf84251", "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7", True)

    , pool("Ethereum", "OHMv1-DAI", "Sushiswap", "0x34d7d7aaf50ad4944b70b320acb24c95fa2def7c", "0x383518188c0c6d7730d91b2c03a03c837814a899", "0x6b175474e89094c44da98b954eedeac495271d0f", True)
    , pool("Ethereum", "OHM-DAI", "Sushiswap", "0x055475920a8c93cffb64d039a8205f7acc7722d3", "0x64aa3364f17a4d01c6f1751fd97c2bd3d7e7f1d5", "0x6b175474e89094c44da98b954eedeac495271d0f", True)
    , pool("Ethereum", "OHM-BTRFLY", "Sushiswap", "0xe9ab8038ee6dd4fcc7612997fe28d4e22019c4b4", "0x64aa3364f17a4d01c6f1751fd97c2bd3d7e7f1d5", "0xc0d4ceb216b3ba9c3701b291766fdcba977cec3a", True)
    , pool("Ethereum", "OHMv1-BTRFLY", "Sushiswap", "0x96f8c74707c544f654e02e098bb83f69640241b6", "0x383518188c0c6d7730d91b2c03a03c837814a899", "0xc0d4ceb216b3ba9c3701b291766fdcba977cec3a", True)
    , pool("Ethereum", "OHM-LOBI", "Sushiswap", "0x193008eaade86658df8237a436261e23e3bcbbaa", "0x64aa3364f17a4d01c6f1751fd97c2bd3d7e7f1d5", "0xdec41db0c33f3f6f3cb615449c311ba22d418a8d", True)
    , pool("Ethereum", "LUSD-OHM", "Sushiswap", "0x46e4d8a1322b9448905225e52f914094dbd6dddf", "0x5f98805a4e8be255a32880fdec7f6728c6568ba0", "0x64aa3364f17a4d01c6f1751fd97c2bd3d7e7f1d5", True)
    , pool("Ethereum", "OHMv1-WETH", "Sushiswap", "0xfffae4a0f4ac251f4705717cd24cadccc9f33e06", "0x383518188c0c6d7730d91b2c03a03c837814a899", "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", True)
    , pool("Ethereum", "OHMv1-LUSD", "Sushiswap", "0xfdf12d1f85b5082877a6e070524f50f6c84faa6b", "0x383518188c0c6d7730d91b2c03a03c837814a899", "0x5f98805A4E8be255a32880FDeC7F6728C6568bA0", True)
    , pool("Ethereum", "OHMv1-FRAX", "Sushiswap", "0x6c765d6b957dacac398d0996cc3e32bd599c8f79", "0x383518188c0c6d7730d91b2c03a03c837814a899", "0x853d955acef822db058eb8505911ed77f175b99e", True)
    , pool("Ethereum", "OHMv1-LOBI", "Sushiswap", "0x2734f4a846d1127f4b5d3bab261facfe51df1d9a", "0x383518188c0c6d7730d91b2c03a03c837814a899", "0xdec41db0c33f3f6f3cb615449c311ba22d418a8d", True)
    , pool("Ethereum", "wsOHM-FDT", "Sushiswap", "0x2e30e758b3950dd9afed2e21f5ab82156fbdbbba", "0xca76543cf381ebbb277be79574059e32108e3e65", "0xed1480d12be41d92f36f5f7bdd88212e381a3677", True)
    , pool("Ethereum", "MNFST-OHMv1", "Sushiswap", "0x89c4d11dfd5868d847ca26c8b1caa9c25c712cef", "0x21585bbcd5bdc3f5737620cf0db2e51978cf60ac", "0x383518188c0c6d7730d91b2c03a03c837814a899", True)
    , pool("Ethereum", "WETH-wsOHM", "Sushiswap", "0xe7ff7ad20dbd69164607f793409283678b5c27c3", "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", "0xca76543cf381ebbb277be79574059e32108e3e65", True)
    , pool("Ethereum", "alphaOHM-OHMv1", "Sushiswap", "0xdc6524f9dce9840895d46dda1fac432ed81182b8", "0x24ecfd535675f36ba1ab9c5d39b50dc097b0792e", "0x383518188c0c6d7730d91b2c03a03c837814a899", True)
    , pool("Ethereum", "OHMv1-HADES", "Sushiswap", "0xf9551f1dea1e2aec80a9c9dad6251e0de531551f", "0x383518188c0c6d7730d91b2c03a03c837814a899", "0xa48d2070472ad4d872d2f5d5893488c763424e09", True)
    , pool("Ethereum", "OHMv1-OHMEOW", "Sushiswap", "0xb0ed0ff7461d11894f17587694153049aa0003a6", "0x383518188c0c6d7730d91b2c03a03c837814a899", "0x72aa40952217bb1ae7b27ea567207cb10af756e8", True)
    , pool("Ethereum", "OHMv1-DEMOHM", "Sushiswap", "0x43ae5e3b718049aabecf1ca9e276b903b6a6d93f", "0x383518188c0c6d7730d91b2c03a03c837814a899", "0xca2b663391684a57fb69f4be1aa8d0dde927b685", True)
    , pool("Ethereum", "OHMv1-CATOHM", "Sushiswap", "0x415bfde2763ed84dd457201ae9b462d252484740", "0x383518188c0c6d7730d91b2c03a03c837814a899", "0xba3dc9c2cf1c666cf991bfbbef77bb5f9c7f93b5", True)
    , pool("Ethereum", "gOHM-FDT", "Sushiswap", "0x75b02b9889536b617d57d08c1ccb929c523945c1", "0x0ab87046fBb341D058F17CBC4c1133F25a20a52f", "0xed1480d12be41d92f36f5f7bdd88212e381a3677", True)
    , pool("Ethereum", "sOHM-LUSD", "Sushiswap", "0x20062da66f4d36942313c3395e5936cf04e8320a", "0x04f2694c8fcee23e8fd0dfea1d4f5bb8c352111f", "0x5f98805A4E8be255a32880FDeC7F6728C6568bA0", True)
    , pool("Ethereum", "sOHM-WETH", "Sushiswap", "0xd83622310acf9b9e49ff85664304c42a38c13404", "0x04f2694c8fcee23e8fd0dfea1d4f5bb8c352111f", "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", True)
    , pool("Ethereum", "OHM-WETH", "Sushiswap", "0x69b81152c5a8d35a67b32a4d3772795d96cae4da", "0x64aa3364f17a4d01c6f1751fd97c2bd3d7e7f1d5", "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", True)
    , pool("Ethereum", "OHM-FRAX", "Sushiswap", "0x04fc84d0b2914005cd16fa377ae889d768b2e7ff", "0x64aa3364f17a4d01c6f1751fd97c2bd3d7e7f1d5", "0x853d955acef822db058eb8505911ed77f175b99e", True)
    , pool("Ethereum", "OHM-USDT", "Sushiswap", "0x800930a57c28845026d18514ce29aaf644dc343e", "0x64aa3364f17a4d01c6f1751fd97c2bd3d7e7f1d5", "0xdac17f958d2ee523a2206206994597c13d831ec7", True)
    , pool("Ethereum", "gOHM-WETH", "Sushiswap", "0x8ed26a97ca17ae8f652f182091475ffa9b07b084", "0x0ab87046fbb341d058f17cbc4c1133f25a20a52f", "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", True)
    , pool("Ethereum", "gOHM-COMMA", "Sushiswap", "0x707d3f011ffd7eb52f0b9b4ce833913bf80e2dc3", "0x0ab87046fbb341d058f17cbc4c1133f25a20a52f", "0x6bd599a8b945074a375bf6bdbb7abe3126603cb6", True)

    , pool("Ethereum", "OHMv1-FRAX", "UniswapV2", "0x2dce0dda1c2f98e0f171de8333c3c6fe1bbf4877", "0x383518188c0c6d7730d91b2c03a03c837814a899", "0x853d955acef822db058eb8505911ed77f175b99e", True)
    , pool("Ethereum", "OHM-FRAX", "UniswapV2", "0xb612c37688861f1f90761dc7f382c2af3a50cc39", "0x64aa3364f17a4d01c6f1751fd97c2bd3d7e7f1d5", "0x853d955acef822db058eb8505911ed77f175b99e", True)
    , pool("Ethereum", "RENA-wsOHM", "UniswapV2", "0xbb26cc4b6ebe045afbc3177a7d091f2368e8ddb0", "0x56de8bc61346321d4f2211e3ac3c0a7f00db9b76", "0xca76543cf381ebbb277be79574059e32108e3e65", True)
    , pool("Ethereum", "alphaOHM-OHMv1", "UniswapV2", "0x6ece69c39bd295e5802901ba5959b8275ac6bd93", "0x24ecfd535675f36ba1ab9c5d39b50dc097b0792e", "0x383518188c0c6d7730d91b2c03a03c837814a899", True)
    , pool("Ethereum", "OHMv1-LUSD", "UniswapV2", "0x6255550a53fd12b7131c0829dc439c5e02eebca8", "0x383518188c0c6d7730d91b2c03a03c837814a899", "0x5f98805A4E8be255a32880FDeC7F6728C6568bA0", True)
    , pool("Ethereum", "OHMv1-DEMOHM", "UniswapV2", "0xdc516f5699e6224d014e2e02c5c1a6ab3549422a", "0x383518188c0c6d7730d91b2c03a03c837814a899", "0xca2b663391684a57fb69f4be1aa8d0dde927b685", True)
    , pool("Ethereum", "OHM-WETH", "UniswapV2", "0x88b8555cb3fdee7077491e673a5bddfb7144744f", "0x64aa3364f17a4d01c6f1751fd97c2bd3d7e7f1d5", "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", True)
    , pool("Ethereum", "OHMv1-TT", "UniswapV2", "0xd0eff8a78da2023f229002a69d3cc7bbc4a814cb", "0x383518188c0c6d7730d91b2c03a03c837814a899", "0x8bdfd0a2990876991c464ebe0491618e39212acb", True)
    , pool("Ethereum", "gOHM-RENA", "UniswapV2", "0xbaf209e70f2255986843c46984efbcef24cd28be", "0x0ab87046fbb341d058f17cbc4c1133f25a20a52f", "0x56de8bc61346321d4f2211e3ac3c0a7f00db9b76", True)
    , pool("Ethereum", "gOHM-VBTC", "UniswapV2", "0x7db1187784db6008dd6ee213cd570a6b93969fe5", "0x0ab87046fbb341d058f17cbc4c1133f25a20a52f", "0xe1406825186d63980fd6e2ec61888f7b91c4bae4", True)

    , pool("Ethereum", "OHMv1-WETH", "UniswapV3", "0xf1b63cd9d80f922514c04b0fd0a30373316dd75b", "0x383518188c0c6d7730d91b2c03a03c837814a899", "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", True)
    , pool("Ethereum", "OHMv1-USDC", "UniswapV3", "0x8406cb08a52afd2a97e958b8fad2103243b6af3e", "0x383518188c0c6d7730d91b2c03a03c837814a899", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", True)
    , pool("Ethereum", "OHMv1-USDC", "UniswapV3", "0x6934290e0f75f64b83c3f473f65aefe97807103b", "0x383518188c0c6d7730d91b2c03a03c837814a899", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", True)
    , pool("Ethereum", "gOHM-WETH", "UniswapV3", "0xf1fd5ae496f6ef1874fd8661b52d762b00686012", "0x0ab87046fbb341d058f17cbc4c1133f25a20a52f", "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", True)

    , pool("Fantom", "HEC-gOHM", "Spookyswap", "0x90cc0314023b6acf7b5844a27240bc0aa2064ebc", "0x5c4fdfc5233f935f20d2adba572f770c2e377ab0", "0x91fa20244fb509e8289ca630e5db3e9166233fdc", True)
    , pool("Fantom", "HEC-gOHM", "Spookyswap", "0xeb7942e26368b2052cbbda2c054482f00436ef7b", "0x5c4fdfc5233f935f20d2adba572f770c2e377ab0", "0x91fa20244fb509e8289ca630e5db3e9166233fdc", True)
    , pool("Fantom", "FTM-gOHM", "Spiritswap", "0xae9bba22e87866e48ccacff0689afaa41eb94995", "0x21be370d5312f44cb42ce377bc9b8a0cef1a4c83", "0x91fa20244fb509e8289ca630e5db3e9166233fdc", True)
    , pool("Fantom", "gOHM-wsSQUID", "Spiritswap", "0x292e3cf358c40c38156f874ac4fc726f72543e92", "0x91fa20244fb509e8289ca630e5db3e9166233fdc", "0xb280458b3cf0facc33671d52fb0e894447c2539a", True)
    , pool("Fantom", "POOP-gOHM", "Spiritswap", "0x861efb3eae9a878a1d52cfd8b1633ff69050e7cd", "0x070eb1a48725622de867a7e3d1dd4f0108966ed1", "0x91fa20244fb509e8289ca630e5db3e9166233fdc", True)

    , pool("Polygon", "wsKLIMA-gOHM", "Sushiswap", "0x1e8126d59adb8b8b683be83333ced47adfed4a74", "0x6f370dba99e32a3cad959b341120db3c9e280ba6", "0xd8ca34fd379d9ca3c6ee3b3905678320f5b45195", True)
    , pool("Polygon", "WMATIC-gOHM", "Sushiswap", "0x65edb37cd6934d8eb825c0ceb6cfbbc33a1935d1", "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270", "0xd8ca34fd379d9ca3c6ee3b3905678320f5b45195", True)
    , pool("Polygon", "FRAX-gOHM", "Sushiswap", "0x7b6146aa0723df09c0e337c980417fdb5a1f8fa2", "0x45c32fa6df82ead1e2ef74d17b76547eddfaff89", "0xd8ca34fd379d9ca3c6ee3b3905678320f5b45195", True)
    , pool("Polygon", "DAI-gOHM", "Sushiswap", "0x7c8bedace24070334de421506cdf9a2e4809f076", "0x8f3cf7ad23cd3cadbd9735aff958023239c6a063", "0xd8ca34fd379d9ca3c6ee3b3905678320f5b45195", True)
    , pool("Polygon", "KLIMA-gOHM", "Sushiswap", "0xc2850f808d30d12592ba311b7ead0b659b938304", "0x4e78011ce80ee02d2c3e649fb657e45898257815", "0xd8ca34fd379d9ca3c6ee3b3905678320f5b45195", True)
    , pool("Polygon", "USDC-gOHM", "Sushiswap", "0x483623a4057357fdfb5e5589ec46e10f130bb0f8", "0x2791bca1f2de4661ed88a30c99a7a9449aa84174", "0xd8ca34fd379d9ca3c6ee3b3905678320f5b45195", True)
    , pool("Polygon", "WETH-gOHM", "Sushiswap", "0x1549e0e8127d380080aab448b82d280433ce4030", "0x7ceb23fd6bc0add59e62ac25578270cff1b9f619", "0xd8ca34fd379d9ca3c6ee3b3905678320f5b45195", True)
    , pool("Polygon", "SUSHI-gOHM", "Sushiswap", "0xb85faf3a22b21855ebd9be31eaf87f4d3673e354", "0x0b3f868e0be5597d5db7feb59e1cadbb0fdda50a", "0xd8ca34fd379d9ca3c6ee3b3905678320f5b45195", True)
    , pool("Polygon", "WMATIC-gOHM", "Sushiswap", "0x3f9650f4a2339b87aca35461a47f030c9cae7d38", "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270", "0xd8ca34fd379d9ca3c6ee3b3905678320f5b45195", True)
    , pool("Polygon", "WETH-gOHM", "Sushiswap", "0x936a59c6194f72a790bf609d3d1c181e61355cc0", "0x7ceb23fd6bc0add59e62ac25578270cff1b9f619", "0xd8ca34fd379d9ca3c6ee3b3905678320f5b45195", True)
    , pool("Polygon", "ABI-gOHM", "Sushiswap", "0x85de09d3ec26131bae6b2d111b63b47d96ab61e7", "0x6d5f5317308c6fe7d6ce16930353a8dfd92ba4d7", "0xd8ca34fd379d9ca3c6ee3b3905678320f5b45195", True)
]


_just_addresses = [pool.address for pool in pools]
assert len(_just_addresses) == len(set(_just_addresses)), f"Duplicate values in asset addresses: {[item for item, count in Counter(_just_addresses).items() if count > 1]}"

chain_explorers = {
    "Avalanche, TraderJoe": "https://analytics.traderjoexyz.com/pairs/"
    , "Avalanche, Pangolin": "https://info.pangolin.exchange/#/pair/"
    , "Avalanche, Sushiswap": ""
    , "Ethereum, Sushiswap": "https://analytics.sushi.com/pairs/"
    , "Ethereum, UniswapV2": "https://v2.info.uniswap.org/pair/"
    , "Ethereum, UniswapV3": "https://info.uniswap.org/#/pools/"
    , "Arbitrum, Sushiswap": "https://analytics-arbitrum.sushi.com/pairs/"
    , "Fantom, Spookyswap": "https://info.spookyswap.finance/pair/"
    , "Fantom, Spiritswap": "https://info.spiritswap.finance/pair/"
    , "Fantom, Sushiswap": "https://app.sushi.com/fr/analytics/pairs/"
    , "Polygon, Sushiswap": "https://analytics-polygon.sushi.com/pairs/"
}
