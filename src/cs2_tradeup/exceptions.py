class TradeUpError(Exception):
    pass


class InvalidContractError(TradeUpError):
    pass


class MissingUpgradePathError(TradeUpError):
    pass


class MissingPriceError(TradeUpError):
    pass


class MarketAPIError(TradeUpError):
    pass


class MarketRequestError(MarketAPIError):
    pass


class MarketParseError(MarketAPIError):
    pass


class FormulaGenerationError(TradeUpError):
    pass


class ImpossibleExteriorError(FormulaGenerationError):
    pass


class PriceLookupError(TradeUpError):
    pass
