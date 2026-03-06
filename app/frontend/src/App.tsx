import { useState, useEffect, useMemo, useRef } from 'react'
import { BrowserRouter, Routes, Route, NavLink, useLocation, useParams } from 'react-router-dom'
import {
  LayoutDashboard,
  Zap,
  TrendingUp,
  TrendingDown,
  MessageSquare,
  Sparkles,
  Bell,
  Activity,
  ArrowRightLeft,
  BarChart2,
  Battery,
  BatteryCharging,
  Moon,
  Sun,
  Thermometer,
  DollarSign,
  Leaf,
  Brain,
  Database,
  Clock,
  Radio,
  Building2,
  Wrench,
  Home as HomeIcon,
  Settings as SettingsIcon,
  Flame,
  Users,
  Network,
  Shield,
  ShieldAlert,
  Calendar,
  Gauge,
  FileText,
  Droplets,
  AlertTriangle,
  Target,
  MapPin,
  GitBranch,
  Receipt,
  Wifi,
  Wind,
  AlertOctagon,
  Heart,
  CloudLightning,
  Cpu,
  Atom,
  Fuel,
  TreePine,
  Anchor,
  Car,
  Award,
  Layers,
  BookOpen,
  Tag,
  Waves,
  CircuitBoard,
  BarChart3,
  BarChart,
  SunMedium,
  Tornado,
  Factory,
  RefreshCw,
  ArrowLeftRight,
  Map as MapIcon,
  GitMerge,
  Globe,
  Globe2,
  ShieldCheck,
  CreditCard,
  ShoppingCart,
  Sliders,
  Power,
  Scale,
  Lock,
  Smartphone,
  Truck,
  Grid,
  Ship,
  CheckCircle,
  Monitor,
  ShoppingBag,
  Eye,
  Plug,
  PieChart,
  AlertCircle,
  Building,
  UserMinus,
  PauseCircle,
  Gavel,
  Cloud,
  Briefcase,
  Upload,
  Rocket,
  CloudRain,
} from 'lucide-react'

import ElectricityMarketCompetitionConcentrationAnalytics from './pages/ElectricityMarketCompetitionConcentrationAnalytics'
import Home from './pages/Home'
import LiveMarket from './pages/LiveMarket'
import Forecasts from './pages/Forecasts'
import Copilot from './pages/Copilot'
import Genie from './pages/Genie'
import Alerts from './pages/Alerts'
import Monitoring from './pages/Monitoring'
import MarketDepth from './pages/MarketDepth'
import PriceAnalysis from './pages/PriceAnalysis'
import Interconnectors from './pages/Interconnectors'
import GeneratorFleet from './pages/GeneratorFleet'
import MarketNotices from './pages/MarketNotices'
import WeatherDemand from './pages/WeatherDemand'
import BessAnalytics from './pages/BessAnalytics'
import TradingDesk from './pages/TradingDesk'
import Sustainability from './pages/Sustainability'
import MeritOrder from './pages/MeritOrder'
import MlDashboardPage from './pages/MlDashboard'
import DataCatalog from './pages/DataCatalog'
import ScenarioAnalysis from './pages/ScenarioAnalysis'
import LoadDuration from './pages/LoadDuration'
import HistoricalTrends from './pages/HistoricalTrends'
import FrequencyAnalytics from './pages/FrequencyAnalytics'
import EnergyFutures from './pages/EnergyFutures'
import ParticipantRegistry from './pages/ParticipantRegistry'
import OutageSchedule from './pages/OutageSchedule'
import DerDashboard from './pages/DerDashboard'
import Settings from './pages/Settings'
import GasMarket from './pages/GasMarket'
import RetailMarket from './pages/RetailMarket'
import NetworkAnalytics from './pages/NetworkAnalytics'
import RezInfrastructure from './pages/RezInfrastructure'
import CurtailmentAnalytics from './pages/CurtailmentAnalytics'
import DemandResponse from './pages/DemandResponse'
import SystemSecurity from './pages/SystemSecurity'
import BiddingAnalytics from './pages/BiddingAnalytics'
import NemEvents from './pages/NemEvents'
import FcasMarket from './pages/FcasMarket'
import BatteryEconomics from './pages/BatteryEconomics'
import NemSettlement from './pages/NemSettlement'
import CarbonAnalytics from './pages/CarbonAnalytics'
import HedgingAnalytics from './pages/HedgingAnalytics'
import HydroStorage from './pages/HydroStorage'
import MarketPower from './pages/MarketPower'
import PasaAnalytics from './pages/PasaAnalytics'
import SraAuction from './pages/SraAuction'
import PpaMarket from './pages/PpaMarket'
import DispatchAccuracy from './pages/DispatchAccuracy'
import RegulatoryTracker from './pages/RegulatoryTracker'
import IspTracker from './pages/IspTracker'
import SolarEvAnalytics from './pages/SolarEvAnalytics'
import LrmcAnalytics from './pages/LrmcAnalytics'
import NetworkConstraints from './pages/NetworkConstraints'
import PriceSetterAnalytics from './pages/PriceSetterAnalytics'
import TariffAnalytics from './pages/TariffAnalytics'
import GridModernisation from './pages/GridModernisation'
import WemOverview from './pages/WemOverview'
import MarketSurveillance from './pages/MarketSurveillance'
import CerDashboard from './pages/CerDashboard'
import SafeguardAnalytics from './pages/SafeguardAnalytics'
import PhesAnalytics from './pages/PhesAnalytics'
import OffshoreWind from './pages/OffshoreWind'
import CauserPays from './pages/CauserPays'
import SpotCapAnalytics from './pages/SpotCapAnalytics'
import TnspAnalytics from './pages/TnspAnalytics'
import DnspPerformanceAnalytics from './pages/DnspPerformanceAnalytics'
import ReliabilityStandardAnalytics from './pages/ReliabilityStandardAnalytics'
import InertiaAnalytics from './pages/InertiaAnalytics'
import HydrogenAnalytics from './pages/HydrogenAnalytics'
import TransmissionProjects from './pages/TransmissionProjects'
import DnspAnalytics from './pages/DnspAnalytics'
import VppDashboard from './pages/VppDashboard'
import MarketReformTracker from './pages/MarketReformTracker'
import TuosAnalytics from './pages/TuosAnalytics'
import CarbonRegistry from './pages/CarbonRegistry'
import EvCharging from './pages/EvCharging'
import StorageArbitrage from './pages/StorageArbitrage'
import DemandForecastAnalytics from './pages/DemandForecastAnalytics'
import RezDevelopment from './pages/RezDevelopment'
import CongestionAnalytics from './pages/CongestionAnalytics'
import EnergyEquity from './pages/EnergyEquity'
import DemandResponseAnalytics from './pages/DemandResponseAnalytics'
import BehindTheMeter from './pages/BehindTheMeter'
import RabAnalytics from './pages/RabAnalytics'
import NemRealTimeDashboard from './pages/NemRealTimeDashboard'
import RitAnalytics from './pages/RitAnalytics'
import ForwardCurveAnalytics from './pages/ForwardCurveAnalytics'
import CoalRetirement from './pages/CoalRetirement'
import GasGenEconomics from './pages/GasGenEconomics'
import ConsumerProtection from './pages/ConsumerProtection'
import GeneratorAvailability from './pages/GeneratorAvailability'
import ClimateRiskAnalytics from './pages/ClimateRiskAnalytics'
import SmartGridAnalytics from './pages/SmartGridAnalytics'
import MinimumDemandAnalytics from './pages/MinimumDemandAnalytics'
import MarketEventsAnalysis from './pages/MarketEventsAnalysis'
import BatteryTechAnalytics from './pages/BatteryTechAnalytics'
import CommunityEnergy from './pages/CommunityEnergy'
import AssetManagement from './pages/AssetManagement'
import DecarbonizationPathway from './pages/DecarbonizationPathway'
import NuclearLongDuration from './pages/NuclearLongDuration'
import NuclearEnergyAnalytics from './pages/NuclearEnergyAnalytics'
import BiddingBehaviour from './pages/BiddingBehaviour'
import EnergyPoverty from './pages/EnergyPoverty'
import SpotForecastDashboard from './pages/SpotForecastDashboard'
import HydrogenEconomy from './pages/HydrogenEconomy'
import CarbonCreditMarket from './pages/CarbonCreditMarket'
import GridResilience from './pages/GridResilience'
import EvFleetCharging from './pages/EvFleetCharging'
import RecMarket from './pages/RecMarket'
import TransmissionCongestion from './pages/TransmissionCongestion'
import DermsOrchestration from './pages/DermsOrchestration'
import MarketDesignReform from './pages/MarketDesignReform'
import RezCapacityTracking from './pages/RezCapacityTracking'
import RetailOfferComparison from './pages/RetailOfferComparison'
import SystemOperatorActions from './pages/SystemOperatorActions'
import OffshoreWindPipeline from './pages/OffshoreWindPipeline'
import NetworkTariffReform from './pages/NetworkTariffReform'
import PriceSpikeAnalysis from './pages/PriceSpikeAnalysis'
import StorageRevenueStack from './pages/StorageRevenueStack'
import SolarResourceAnalytics from './pages/SolarResourceAnalytics'
import FuturesMarketRisk from './pages/FuturesMarketRisk'
import WindResourceAnalytics from './pages/WindResourceAnalytics'
import CorporatePpaMarket from './pages/CorporatePpaMarket'
import MicrogridRaps from './pages/MicrogridRaps'
import MarketLiquidity from './pages/MarketLiquidity'
import ThermalEfficiency from './pages/ThermalEfficiency'
import IndustrialDemandFlex from './pages/IndustrialDemandFlex'
import StorageLca from './pages/StorageLca'
import InterconnectorFlowAnalytics from './pages/InterconnectorFlowAnalytics'
import IspProgressTracker from './pages/IspProgressTracker'
import FirmingTechnologyEconomics from './pages/FirmingTechnologyEconomics'
import DemandForecastingModels from './pages/DemandForecastingModels'
import MarketStressTesting from './pages/MarketStressTesting'
import CapacityInvestmentSignals from './pages/CapacityInvestmentSignals'
import FrequencyControlAnalytics from './pages/FrequencyControlAnalytics'
import RecCertificateTracking from './pages/RecCertificateTracking'
import RenewableIntegrationCost from './pages/RenewableIntegrationCost'
import SpotMarketDepthAnalytics from './pages/SpotMarketDepthAnalytics'
import StorageTechRoadmap from './pages/StorageTechRoadmap'
import PlannedOutageAnalytics from './pages/PlannedOutageAnalytics'
import MarketShareTracker from './pages/MarketShareTracker'
import VolatilityRegimeAnalytics from './pages/VolatilityRegimeAnalytics'
import BlackStartCapability from './pages/BlackStartCapability'
import AncillaryServicesCost from './pages/AncillaryServicesCost'
import CbamTradeAnalytics from './pages/CbamTradeAnalytics'
import CongestionRevenueAnalytics from './pages/CongestionRevenueAnalytics'
import ClimatePhysicalRisk from './pages/ClimatePhysicalRisk'
import EnergyAffordabilityAnalytics from './pages/EnergyAffordabilityAnalytics'
import ElectrificationAnalytics from './pages/ElectrificationAnalytics'
import ElectricityExportInfra from './pages/ElectricityExportInfra'
import ElectricityExportEconomicsAnalytics from './pages/ElectricityExportEconomicsAnalytics'
import LdesEconomicsAnalytics from './pages/LdesEconomicsAnalytics'
import GasTransitionAnalytics from './pages/GasTransitionAnalytics'
import ProsumerAnalytics from './pages/ProsumerAnalytics'
import StorageOptimisationAnalytics from './pages/StorageOptimisationAnalytics'
import SettlementAnalytics from './pages/SettlementAnalytics'
import RealtimeOperationsDashboard from './pages/RealtimeOperationsDashboard'
import RenewableAuctionAnalytics from './pages/RenewableAuctionAnalytics'
import VollAnalytics from './pages/VollAnalytics'
import DemandFlexibilityAnalytics from './pages/DemandFlexibilityAnalytics'
import FuturesPriceDiscovery from './pages/FuturesPriceDiscovery'
import ElectricityPriceIndex from './pages/ElectricityPriceIndex'
import InterconnectorUpgradeAnalytics from './pages/InterconnectorUpgradeAnalytics'
import MlfAnalytics from './pages/MlfAnalytics'
import CspAnalytics from './pages/CspAnalytics'
import CarbonIntensityAnalytics from './pages/CarbonIntensityAnalytics'
import NetworkTariffReformAnalytics from './pages/NetworkTariffReformAnalytics'
import TariffCrossSubsidyAnalytics from './pages/TariffCrossSubsidyAnalytics'
import AiDigitalTwinAnalytics from './pages/AiDigitalTwinAnalytics'
import EsooAdequacyAnalytics from './pages/EsooAdequacyAnalytics'
import SocialLicenceAnalytics from './pages/SocialLicenceAnalytics'
import ElectricityOptionsAnalytics from './pages/ElectricityOptionsAnalytics'
import GridFormingInverterAnalytics from './pages/GridFormingInverterAnalytics'
import CapacityMechanismAnalytics from './pages/CapacityMechanismAnalytics'
import DemandForecastAccuracyAnalytics from './pages/DemandForecastAccuracyAnalytics'
import TransmissionInvestmentAnalytics from './pages/TransmissionInvestmentAnalytics'
import RezProgressAnalytics from './pages/RezProgressAnalytics'
import StorageRevenueAnalytics from './pages/StorageRevenueAnalytics'
import CarbonPricePathwayAnalytics from './pages/CarbonPricePathwayAnalytics'
import SpotPriceForecastAnalytics from './pages/SpotPriceForecastAnalytics'
import AncillaryCostAllocationAnalytics from './pages/AncillaryCostAllocationAnalytics'
import MarketLiquidityAnalytics from './pages/MarketLiquidityAnalytics'
import GeneratorRetirementAnalytics from './pages/GeneratorRetirementAnalytics'
import ConsumerHardshipAnalytics from './pages/ConsumerHardshipAnalytics'
import DsrAggregatorAnalytics from './pages/DsrAggregatorAnalytics'
import PowerSystemEventsAnalytics from './pages/PowerSystemEventsAnalytics'
import MerchantRenewableAnalytics from './pages/MerchantRenewableAnalytics'
import RetailerCompetitionAnalytics from './pages/RetailerCompetitionAnalytics'
import StorageCostCurvesAnalytics from './pages/StorageCostCurvesAnalytics'
import ExtremeWeatherResilienceAnalytics from './pages/ExtremeWeatherResilienceAnalytics'
import SpotPriceVolatilityRegimeAnalytics from './pages/SpotPriceVolatilityRegimeAnalytics'
import IndustrialElectrificationAnalytics from './pages/IndustrialElectrificationAnalytics'
import OffshoreWindDevAnalytics from './pages/OffshoreWindDevAnalytics'
import PumpedHydroResourceAssessmentAnalytics from './pages/PumpedHydroResourceAssessmentAnalytics'
import FrequencyControlPerformanceAnalytics from './pages/FrequencyControlPerformanceAnalytics'
import CostReflectiveTariffReformAnalytics from './pages/CostReflectiveTariffReformAnalytics'
import NEMMarketMicrostructureAnalytics from './pages/NEMMarketMicrostructureAnalytics'
import EVFleetGridImpactAnalytics from './pages/EVFleetGridImpactAnalytics'
import HydrogenEconomyAnalytics from './pages/HydrogenEconomyAnalytics'
import RooftopSolarGridAnalytics from './pages/RooftopSolarGridAnalytics'
import RECMarketAnalytics from './pages/RECMarketAnalytics'
import EnergyPovertyAnalytics from './pages/EnergyPovertyAnalytics'
import HedgeEffectivenessAnalytics from './pages/HedgeEffectivenessAnalytics'
import CBAMTradeExposureAnalytics from './pages/CBAMTradeExposureAnalytics'
import DemandResponseProgramAnalytics from './pages/DemandResponseProgramAnalytics'
import InterconnectorCongestionAnalytics from './pages/InterconnectorCongestionAnalytics'
import PPAMarketAnalytics from './pages/PPAMarketAnalytics'
import PPAStructuringAnalytics from './pages/PPAStructuringAnalytics'
import BatteryDispatchStrategyAnalytics from './pages/BatteryDispatchStrategyAnalytics'
import GenerationMixTransitionAnalytics from './pages/GenerationMixTransitionAnalytics'
import StorageDurationEconomicsAnalytics from './pages/StorageDurationEconomicsAnalytics'
import AncillaryServicesMarketDepthAnalytics from './pages/AncillaryServicesMarketDepthAnalytics'
import SRAAnalyticsPage from './pages/SRAAnalyticsPage'
import SpotMarketStressAnalytics from './pages/SpotMarketStressAnalytics'
import ElectricityWorkforceAnalytics from './pages/ElectricityWorkforceAnalytics'
import REZTransmissionAnalytics from './pages/REZTransmissionAnalytics'
import NetworkRegulatoryFrameworkAnalytics from './pages/NetworkRegulatoryFrameworkAnalytics'
import NetworkInvestmentPipelineAnalytics from './pages/NetworkInvestmentPipelineAnalytics'
import NEMDemandForecastAnalytics from './pages/NEMDemandForecastAnalytics'
import HydrogenFuelCellVehicleAnalytics from './pages/HydrogenFuelCellVehicleAnalytics'
import PriceModelComparisonAnalytics from './pages/PriceModelComparisonAnalytics'
import GasElectricityNexusAnalytics from './pages/GasElectricityNexusAnalytics'
import BiddingComplianceAnalytics from './pages/BiddingComplianceAnalytics'
import CommunityEnergyAnalytics from './pages/CommunityEnergyAnalytics'
import GridCybersecurityAnalytics from './pages/GridCybersecurityAnalytics'
import MarketParticipantFinancialAnalytics from './pages/MarketParticipantFinancialAnalytics'
import DigitalTransformationAnalytics from './pages/DigitalTransformationAnalytics'
import NegativePriceEventAnalytics from './pages/NegativePriceEventAnalytics'
import CEROrchestrationAnalytics from './pages/CEROrchestrationAnalytics'
import EnergyTransitionFinanceAnalytics from './pages/EnergyTransitionFinanceAnalytics'
import SystemLoadBalancingAnalytics from './pages/SystemLoadBalancingAnalytics'
import CarbonAccountingAnalytics from './pages/CarbonAccountingAnalytics'
import WholesaleBiddingStrategyAnalytics from './pages/WholesaleBiddingStrategyAnalytics'
import EmergencyManagementAnalytics from './pages/EmergencyManagementAnalytics'
import LDESAnalytics from './pages/LDESAnalytics'
import ConsumerSwitchingRetailChurnAnalytics from './pages/ConsumerSwitchingRetailChurnAnalytics'
import SolarThermalCSPAnalytics from './pages/SolarThermalCSPAnalytics'
import NEMPostReformMarketDesignAnalytics from './pages/NEMPostReformMarketDesignAnalytics'
import ElectricityPriceForecastingModelAnalytics from './pages/ElectricityPriceForecastingModelAnalytics'
import LargeIndustrialDemandAnalytics from './pages/LargeIndustrialDemandAnalytics'
import SpotPriceSpikePredictionAnalytics from './pages/SpotPriceSpikePredictionAnalytics'
import GridEdgeTechnologyAnalytics from './pages/GridEdgeTechnologyAnalytics'
import EnergyStorageDegradationAnalytics from './pages/EnergyStorageDegradationAnalytics'
import CleanHydrogenProductionCostAnalytics from './pages/CleanHydrogenProductionCostAnalytics'
import AncillaryServicesProcurementAnalytics from './pages/AncillaryServicesProcurementAnalytics'
import REZConnectionQueueAnalytics from './pages/REZConnectionQueueAnalytics'
import AustralianCarbonPolicyAnalytics from './pages/AustralianCarbonPolicyAnalytics'
import MarketDesignSimulationAnalytics from './pages/MarketDesignSimulationAnalytics'
import PowerSystemStabilityAnalytics from './pages/PowerSystemStabilityAnalytics'
import EnergyRetailCompetitionAnalytics from './pages/EnergyRetailCompetitionAnalytics'
import CleanEnergyFinanceAnalytics from './pages/CleanEnergyFinanceAnalytics'
import NuclearEnergyEconomicsAnalytics from './pages/NuclearEnergyEconomicsAnalytics'
import BehindMeterCommercialAnalytics from './pages/BehindMeterCommercialAnalytics'
import CapacityInvestmentSchemeAnalytics from './pages/CapacityInvestmentSchemeAnalytics'
import DemandFlexibilityMarketAnalytics from './pages/DemandFlexibilityMarketAnalytics'
import EnergyAssetLifeExtensionAnalytics from './pages/EnergyAssetLifeExtensionAnalytics'
import GreenAmmoniaExportAnalytics from './pages/GreenAmmoniaExportAnalytics'
import ElectricityExportCableAnalytics from './pages/ElectricityExportCableAnalytics'
import IndustrialDecarbonisationAnalytics from './pages/IndustrialDecarbonisationAnalytics'
import CommunityEnergyStorageAnalytics from './pages/CommunityEnergyStorageAnalytics'
import NEMGenerationMixAnalytics from './pages/NEMGenerationMixAnalytics'
import ConsumerEnergyAffordabilityAnalytics from './pages/ConsumerEnergyAffordabilityAnalytics'
import ElectricityPriceRiskAnalytics from './pages/ElectricityPriceRiskAnalytics'
import EVFleetDepotAnalytics from './pages/EVFleetDepotAnalytics'
import WindFarmWakeAnalytics from './pages/WindFarmWakeAnalytics'
import MarketBiddingStrategyAnalytics from './pages/MarketBiddingStrategyAnalytics'
import SolarPVSoilingAnalytics from './pages/SolarPVSoilingAnalytics'
import OTICSCyberSecurityAnalytics from './pages/OTICSCyberSecurityAnalytics'
import STPASAAdequacyAnalytics from './pages/STPASAAdequacyAnalytics'
import GeneratorPerformanceStandardsAnalytics from './pages/GeneratorPerformanceStandardsAnalytics'
import BiomassBioenergyAnalytics from './pages/BiomassBioenergyAnalytics'
import ElectricityFrequencyPerformanceAnalytics from './pages/ElectricityFrequencyPerformanceAnalytics'
import LGCMarketAnalytics from './pages/LGCMarketAnalytics'
import WaveTidalOceanAnalytics from './pages/WaveTidalOceanAnalytics'
import ReactivePowerVoltageAnalytics from './pages/ReactivePowerVoltageAnalytics'
import BatteryRevenueStackAnalytics from './pages/BatteryRevenueStackAnalytics'
import DigitalEnergyTwinAnalytics from './pages/DigitalEnergyTwinAnalytics'
import NetworkProtectionSystemAnalytics from './pages/NetworkProtectionSystemAnalytics'
import PumpedHydroDispatchAnalytics from './pages/PumpedHydroDispatchAnalytics'
import RetailMarketDesignAnalytics from './pages/RetailMarketDesignAnalytics'
import SpotMarketDepthXAnalytics from './pages/SpotMarketDepthXAnalytics'
import SolarFarmOperationsAnalytics from './pages/SolarFarmOperationsAnalytics'
import DistributionNetworkPlanningAnalytics from './pages/DistributionNetworkPlanningAnalytics'
import GridFlexibilityServicesAnalytics from './pages/GridFlexibilityServicesAnalytics'
import HydrogenRefuellingStationAnalytics from './pages/HydrogenRefuellingStationAnalytics'
import OffshoreWindFinanceAnalytics from './pages/OffshoreWindFinanceAnalytics'
import CarbonOffsetProjectAnalytics from './pages/CarbonOffsetProjectAnalytics'
import PowerGridClimateResilienceAnalytics from './pages/PowerGridClimateResilienceAnalytics'
import EnergyStorageTechComparisonAnalytics from './pages/EnergyStorageTechComparisonAnalytics'
import PowerToXEconomicsAnalytics from './pages/PowerToXEconomicsAnalytics'
import ElectricityMarketMicrostructureAnalytics from './pages/ElectricityMarketMicrostructureAnalytics'
import GridDecarbonisationPathwayAnalytics from './pages/GridDecarbonisationPathwayAnalytics'
import RooftopSolarNetworkImpactAnalytics from './pages/RooftopSolarNetworkImpactAnalytics'
import ElectricityNetworkTariffReformAnalytics from './pages/ElectricityNetworkTariffReformAnalytics'
import LongDurationEnergyStorageAnalytics from './pages/LongDurationEnergyStorageAnalytics'
import HydrogenPipelineInfrastructureAnalytics from './pages/HydrogenPipelineInfrastructureAnalytics'
import CarbonCaptureStorageProjectAnalytics from './pages/CarbonCaptureStorageProjectAnalytics'
import EnergyPovertyVulnerableConsumerAnalytics from './pages/EnergyPovertyVulnerableConsumerAnalytics'
import NuclearSmallModularReactorAnalytics from './pages/NuclearSmallModularReactorAnalytics'
import ElectricityMarketTransparencyAnalytics from './pages/ElectricityMarketTransparencyAnalytics'
import GeothermalEnergyDevelopmentAnalytics from './pages/GeothermalEnergyDevelopmentAnalytics'
import SolarThermalPowerPlantAnalytics from './pages/SolarThermalPowerPlantAnalytics'
import EnergyTradingAlgorithmicStrategyAnalytics from './pages/EnergyTradingAlgorithmicStrategyAnalytics'
import EVGridIntegrationV2GAnalytics from './pages/EVGridIntegrationV2GAnalytics'
import ElectricVehicleGridIntegrationAnalytics from './pages/ElectricVehicleGridIntegrationAnalytics'
import BiomethaneGasGridInjectionAnalytics from './pages/BiomethaneGasGridInjectionAnalytics'
import ElectricityMarketForecastingAccuracyAnalytics from './pages/ElectricityMarketForecastingAccuracyAnalytics'
import NationalEnergyTransitionInvestmentAnalytics from './pages/NationalEnergyTransitionInvestmentAnalytics'
import ElectricitySpotPriceSeasonalityAnalytics from './pages/ElectricitySpotPriceSeasonalityAnalytics'
import GridCongestionConstraintAnalytics from './pages/GridCongestionConstraintAnalytics'
import RenewableEnergyZoneDevelopmentAnalytics from './pages/RenewableEnergyZoneDevelopmentAnalytics'
import BatteryStorageDegradationLifetimeAnalytics from './pages/BatteryStorageDegradationLifetimeAnalytics'
import ElectricityConsumerSwitchingChurnAnalytics from './pages/ElectricityConsumerSwitchingChurnAnalytics'
import NEMInertiaSynchronousCondenserAnalytics from './pages/NEMInertiaSynchronousCondenserAnalytics'
import OffshoreWindLeasingSiteAnalytics from './pages/OffshoreWindLeasingSiteAnalytics'
import ElectricityMarketRegulatoryAppealsAnalytics from './pages/ElectricityMarketRegulatoryAppealsAnalytics'
import DistributedEnergyResourceManagementAnalytics from './pages/DistributedEnergyResourceManagementAnalytics'
import MarketPriceFormationReviewAnalytics from './pages/MarketPriceFormationReviewAnalytics'
import ResidentialSolarSelfConsumptionAnalytics from './pages/ResidentialSolarSelfConsumptionAnalytics'
import EnergyInfrastructureCyberThreatAnalytics from './pages/EnergyInfrastructureCyberThreatAnalytics'
import WholesaleGasMarketAnalytics from './pages/WholesaleGasMarketAnalytics'
import ElectricityDemandForecastingMLAnalytics from './pages/ElectricityDemandForecastingMLAnalytics'
import EnergyStorageMerchantRevenueAnalytics from './pages/EnergyStorageMerchantRevenueAnalytics'
import IndustrialElectrificationXAnalytics from './pages/IndustrialElectrificationXAnalytics'
import TransmissionAccessReformAnalytics from './pages/TransmissionAccessReformAnalytics'
import HydrogenExportTerminalAnalytics from './pages/HydrogenExportTerminalAnalytics'
import EnergyRetailerMarginAnalytics from './pages/EnergyRetailerMarginAnalytics'
import GridEdgeTechnologyXAnalytics from './pages/GridEdgeTechnologyXAnalytics'
import EvFleetChargingAnalytics from './pages/EvFleetChargingAnalytics'
import CarbonBorderAdjustmentAnalytics from './pages/CarbonBorderAdjustmentAnalytics'
import PowerPurchaseAgreementMarketAnalytics from './pages/PowerPurchaseAgreementMarketAnalytics'
import ElectricityMarketLiquidityAnalytics from './pages/ElectricityMarketLiquidityAnalytics'
import DistributedSolarForecastingAnalytics from './pages/DistributedSolarForecastingAnalytics'
import EnergyTransitionFinanceXAnalytics from './pages/EnergyTransitionFinanceXAnalytics'
import NemFrequencyControlAnalytics from './pages/NemFrequencyControlAnalytics'
import BatterySecondLifeAnalytics from './pages/BatterySecondLifeAnalytics'
import UtilitySolarFarmOperationsAnalytics from './pages/UtilitySolarFarmOperationsAnalytics'
import WindFarmWakeEffectAnalytics from './pages/WindFarmWakeEffectAnalytics'
import EnergyPovertyHardshipAnalytics from './pages/EnergyPovertyHardshipAnalytics'
import ElectricityNetworkCapitalInvestmentAnalytics from './pages/ElectricityNetworkCapitalInvestmentAnalytics'
import GasToPowerTransitionAnalytics from './pages/GasToPowerTransitionAnalytics'
import CarbonOffsetMarketAnalytics from './pages/CarbonOffsetMarketAnalytics'
import PowerSystemStabilityXAnalytics from './pages/PowerSystemStabilityXAnalytics'
import AemoMarketOperationsAnalytics from './pages/AemoMarketOperationsAnalytics'
import RenewableEnergyCertificateAnalytics from './pages/RenewableEnergyCertificateAnalytics'
import EnergyStorageDispatchOptimisationAnalytics from './pages/EnergyStorageDispatchOptimisationAnalytics'
import OffshoreWindProjectFinanceAnalytics from './pages/OffshoreWindProjectFinanceAnalytics'
import NationalEnergyMarketReformAnalytics from './pages/NationalEnergyMarketReformAnalytics'
import ElectricityMarketPriceFormationAnalytics from './pages/ElectricityMarketPriceFormationAnalytics'
import RezAuctionCisAnalytics from './pages/RezAuctionCisAnalytics'
import GridModernisationDigitalTwinAnalytics from './pages/GridModernisationDigitalTwinAnalytics'
import EnergyMarketCreditRiskAnalytics from './pages/EnergyMarketCreditRiskAnalytics'
import ElectricityConsumerBehaviourAnalytics from './pages/ElectricityConsumerBehaviourAnalytics'
import ThermalCoalPowerTransitionAnalytics from './pages/ThermalCoalPowerTransitionAnalytics'
import DemandResponseAggregatorAnalytics from './pages/DemandResponseAggregatorAnalytics'
import EnergyCommodityTradingAnalytics from './pages/EnergyCommodityTradingAnalytics'
import NetworkTariffDesignReformAnalytics from './pages/NetworkTariffDesignReformAnalytics'
import HydrogenValleyClusterAnalytics from './pages/HydrogenValleyClusterAnalytics'
import NemCongestionRentAnalytics from './pages/NemCongestionRentAnalytics'
import ElectricityRetailerChurnAnalytics from './pages/ElectricityRetailerChurnAnalytics'
import EnergyAssetMaintenanceAnalytics from './pages/EnergyAssetMaintenanceAnalytics'
import CoalSeamGasAnalytics from './pages/CoalSeamGasAnalytics'
import EvBatteryTechnologyAnalytics from './pages/EvBatteryTechnologyAnalytics'
import NemDemandForecastingAccuracyAnalytics from './pages/NemDemandForecastingAccuracyAnalytics'
import PowerSystemInertiaAnalytics from './pages/PowerSystemInertiaAnalytics'
import ElectricityNetworkInvestmentDeferralAnalytics from './pages/ElectricityNetworkInvestmentDeferralAnalytics'
import RezCapacityFactorAnalytics from './pages/RezCapacityFactorAnalytics'
import EnergyRetailerHedgingAnalytics from './pages/EnergyRetailerHedgingAnalytics'
import GasPowerPlantFlexibilityAnalytics from './pages/GasPowerPlantFlexibilityAnalytics'
import SolarIrradianceResourceAnalytics from './pages/SolarIrradianceResourceAnalytics'
import ElectricityPriceCapInterventionAnalytics from './pages/ElectricityPriceCapInterventionAnalytics'
import BiogasLandfillAnalytics from './pages/BiogasLandfillAnalytics'
import WindResourceVariabilityAnalytics from './pages/WindResourceVariabilityAnalytics'
import EnergyStorageDurationAnalytics from './pages/EnergyStorageDurationAnalytics'
import NemSettlementResidueAuctionAnalytics from './pages/NemSettlementResidueAuctionAnalytics'
import HydrogenElectrolysisCostAnalytics from './pages/HydrogenElectrolysisCostAnalytics'
import ElectricityDemandElasticityAnalytics from './pages/ElectricityDemandElasticityAnalytics'
import NuclearEnergyFeasibilityAnalytics from './pages/NuclearEnergyFeasibilityAnalytics'
import TransmissionCongestionRevenueAnalytics from './pages/TransmissionCongestionRevenueAnalytics'
import ElectricityMarketDesignReformAnalytics from './pages/ElectricityMarketDesignReformAnalytics'
import CarbonCaptureUtilisationAnalytics from './pages/CarbonCaptureUtilisationAnalytics'
import GridScaleBatteryDegradationAnalytics from './pages/GridScaleBatteryDegradationAnalytics'
import AustraliaElectricityExportAnalytics from './pages/AustraliaElectricityExportAnalytics'
import DemandSideManagementProgramAnalytics from './pages/DemandSideManagementProgramAnalytics'
import PowerGridTopologyAnalytics from './pages/PowerGridTopologyAnalytics'
import RooftopSolarFeedInTariffAnalytics from './pages/RooftopSolarFeedInTariffAnalytics'
import LngExportAnalytics from './pages/LngExportAnalytics'
import { useDarkMode } from './hooks/useDarkMode'
import EnergyCommunityMicrogridAnalytics from './pages/EnergyCommunityMicrogridAnalytics'
import ElectricityWholesaleMarketLiquidityAnalytics from './pages/ElectricityWholesaleMarketLiquidityAnalytics'
import TidalWaveMarineEnergyAnalytics from './pages/TidalWaveMarineEnergyAnalytics'
import GeothermalEnergyAnalytics from './pages/GeothermalEnergyAnalytics'
import EnergyStorageArbitrageAnalytics from './pages/EnergyStorageArbitrageAnalytics'
import CarbonBorderAdjustmentXAnalytics from './pages/CarbonBorderAdjustmentXAnalytics'
import FcasProcurementAnalytics from './pages/FcasProcurementAnalytics'
import ElectricityFuturesOptionsAnalytics from './pages/ElectricityFuturesOptionsAnalytics'
import RenewableEnergyCertificateXAnalytics from './pages/RenewableEnergyCertificateXAnalytics'
import DistributedEnergyResourceManagementXAnalytics from './pages/DistributedEnergyResourceManagementXAnalytics'
import CoalMineEnergyAnalytics from './pages/CoalMineEnergyAnalytics'
import NemFiveMinuteSettlementAnalytics from './pages/NemFiveMinuteSettlementAnalytics'
import NetworkCongestionReliefAnalytics from './pages/NetworkCongestionReliefAnalytics'
import MarketConcentrationBiddingAnalytics from './pages/MarketConcentrationBiddingAnalytics'
import IndustrialEnergyEfficiencyAnalytics from './pages/IndustrialEnergyEfficiencyAnalytics'
import RetailerFinancialHealthAnalytics from './pages/RetailerFinancialHealthAnalytics'
import SolarFarmPerformanceAnalytics from './pages/SolarFarmPerformanceAnalytics'
import GasNetworkPipelineAnalytics from './pages/GasNetworkPipelineAnalytics'
import ElectricityPriceForecastingAnalytics from './pages/ElectricityPriceForecastingAnalytics'
import NetworkAssetLifeCycleAnalytics from './pages/NetworkAssetLifeCycleAnalytics'
import WindFarmWakeTurbineAnalytics from './pages/WindFarmWakeTurbineAnalytics'
import EnergyPovertyHardshipXAnalytics from './pages/EnergyPovertyHardshipXAnalytics'
import HydrogenRefuellingTransportAnalytics from './pages/HydrogenRefuellingTransportAnalytics'
import ElectricitySpotPriceEventAnalytics from './pages/ElectricitySpotPriceEventAnalytics'
import LargeScaleRenewableAuctionAnalytics from './pages/LargeScaleRenewableAuctionAnalytics'
import NemAncillaryServicesAnalytics from './pages/NemAncillaryServicesAnalytics'
import AustralianCarbonCreditAnalytics from './pages/AustralianCarbonCreditAnalytics'
import GeneratorCapacityAdequacyAnalytics from './pages/GeneratorCapacityAdequacyAnalytics'
import SmartGridCybersecurityAnalytics from './pages/SmartGridCybersecurityAnalytics'
import PumpedHydroReservoirAnalytics from './pages/PumpedHydroReservoirAnalytics'
import EnergyTransitionJobsAnalytics from './pages/EnergyTransitionJobsAnalytics'
import InterconnectorFlowRightsAnalytics from './pages/InterconnectorFlowRightsAnalytics'
import CleanEnergyFinanceXAnalytics from './pages/CleanEnergyFinanceXAnalytics'
import BessPerformanceAnalytics from './pages/BessPerformanceAnalytics'
import LoadCurveAnalytics from './pages/LoadCurveAnalytics'
import NaturalGasTradingAnalytics from './pages/NaturalGasTradingAnalytics'
import EnergyOptimisationAnalytics from './pages/EnergyOptimisationAnalytics'
import AemcRuleChangeAnalytics from './pages/AemcRuleChangeAnalytics'
import RenewableExportAnalytics from './pages/RenewableExportAnalytics'
import CorporatePpaAnalytics from './pages/CorporatePpaAnalytics'
import NetZeroEmissionsAnalytics from './pages/NetZeroEmissionsAnalytics'
import PriceSensitivityAnalytics from './pages/PriceSensitivityAnalytics'
import GridReliabilityAnalytics from './pages/GridReliabilityAnalytics'
import MarketTradingStrategyAnalytics from './pages/MarketTradingStrategyAnalytics'
import EmergingMarketsAnalytics from './pages/EmergingMarketsAnalytics'
import PortfolioRiskOptimisationAnalytics from './pages/PortfolioRiskOptimisationAnalytics'
import EnergyHubMicrostructureAnalytics from './pages/EnergyHubMicrostructureAnalytics'
import FrequencyReservePlanningAnalytics from './pages/FrequencyReservePlanningAnalytics'
import DistributedAssetOptimisationAnalytics from './pages/DistributedAssetOptimisationAnalytics'
import ConsumerSegmentationAnalytics from './pages/ConsumerSegmentationAnalytics'
import GenerationExpansionAnalytics from './pages/GenerationExpansionAnalytics'
import MarketAnomalyDetectionAnalytics from './pages/MarketAnomalyDetectionAnalytics'
import WindCapacityMarketAnalytics from './pages/WindCapacityMarketAnalytics'
import SolarParkRegistryAnalytics from './pages/SolarParkRegistryAnalytics'
import EnergyStorageDurationXAnalytics from './pages/EnergyStorageDurationXAnalytics'
import OffshoreWindDevelopmentAnalytics from './pages/OffshoreWindDevelopmentAnalytics'
import NemParticipantFinancialAnalytics from './pages/NemParticipantFinancialAnalytics'
import GreenTariffHydrogenAnalytics from './pages/GreenTariffHydrogenAnalytics'
import NemPriceReviewAnalytics from './pages/NemPriceReviewAnalytics'
import RenewableCertificateNemAnalytics from './pages/RenewableCertificateNemAnalytics'
import DemandCurvePriceAnchorAnalytics from './pages/DemandCurvePriceAnchorAnalytics'
import MarketEvolutionPolicyAnalytics from './pages/MarketEvolutionPolicyAnalytics'
import EnergyGridTopologyAnalytics from './pages/EnergyGridTopologyAnalytics'
import RenewableMarketSensitivityAnalytics from './pages/RenewableMarketSensitivityAnalytics'
import CarbonVoluntaryExchangeAnalytics from './pages/CarbonVoluntaryExchangeAnalytics'
import NaturalGasPipelineAnalytics from './pages/NaturalGasPipelineAnalytics'
import BatteryChemistryRiskAnalytics from './pages/BatteryChemistryRiskAnalytics'
import Aemo5MinSettlementAnalytics from './pages/Aemo5MinSettlementAnalytics'
import ElectricityRetailCompetitionAnalytics from './pages/ElectricityRetailCompetitionAnalytics'
import DemandResponseAggregationAnalytics from './pages/DemandResponseAggregationAnalytics'
import GridFrequencyResponseAnalytics from './pages/GridFrequencyResponseAnalytics'
import HydrogenEconomyOutlookAnalytics from './pages/HydrogenEconomyOutlookAnalytics'
import NetworkAugmentationDeferralAnalytics from './pages/NetworkAugmentationDeferralAnalytics'
import EvFleetGridIntegrationAnalytics from './pages/EvFleetGridIntegrationAnalytics'
import GridEmissionsIntensityAnalytics from './pages/GridEmissionsIntensityAnalytics'
import MicrogridResilienceAnalytics from './pages/MicrogridResilienceAnalytics'
import PowerQualityMonitoringAnalytics from './pages/PowerQualityMonitoringAnalytics'
import EnergyPovertyAffordabilityAnalytics from './pages/EnergyPovertyAffordabilityAnalytics'
import VirtualPowerPlantOperationsAnalytics from './pages/VirtualPowerPlantOperationsAnalytics'
import CoalFleetRetirementPathwayAnalytics from './pages/CoalFleetRetirementPathwayAnalytics'
import PumpedHydroStorageAnalytics from './pages/PumpedHydroStorageAnalytics'
import EastCoastGasMarketAnalytics from './pages/EastCoastGasMarketAnalytics'
import SmartMeterDataAnalytics from './pages/SmartMeterDataAnalytics'
import IntegratedSystemPlanAnalytics from './pages/IntegratedSystemPlanAnalytics'
import CommunityBatteryAnalytics from './pages/CommunityBatteryAnalytics'
import EnergySectorCyberSecurityAnalytics from './pages/EnergySectorCyberSecurityAnalytics'
import WholesaleMarketReformAnalytics from './pages/WholesaleMarketReformAnalytics'

const NAV_ITEMS = [
  { to: '/',             label: 'Home',         Icon: LayoutDashboard },
  { to: '/live',         label: 'Live Market',  Icon: Zap             },
  { to: '/realtime-operations', label: 'Live Ops Dashboard', Icon: Radio },
  { to: '/forecasts',    label: 'Forecasts',    Icon: TrendingUp      },
  { to: '/market-depth', label: 'Market Depth', Icon: TrendingUp      },
  { to: '/copilot',      label: 'Copilot',      Icon: MessageSquare   },
  { to: '/genie',        label: 'Genie',        Icon: Sparkles        },
  { to: '/alerts',       label: 'Alerts',       Icon: Bell            },
  { to: '/monitoring',        label: 'Monitoring',     Icon: Activity        },
  { to: '/price-analysis',    label: 'Price Analysis', Icon: BarChart2       },
  { to: '/interconnectors',   label: 'Interconnectors', Icon: ArrowRightLeft },
  { to: '/generator-fleet',  label: 'Generator Fleet', Icon: Zap            },
  { to: '/market-notices',   label: 'Market Notices',   Icon: Bell           },
  { to: '/weather-demand',   label: 'Weather & Demand', Icon: Thermometer    },
  { to: '/bess',             label: 'Battery Storage',  Icon: Battery        },
  { to: '/trading-desk',    label: 'Trading Desk',     Icon: DollarSign     },
  { to: '/sustainability',  label: 'Sustainability',   Icon: Leaf            },
  { to: '/biomass-bioenergy', label: 'Biomass & Bioenergy', Icon: Leaf       },
  { to: '/merit-order',     label: 'Merit Order',      Icon: TrendingUp      },
  { to: '/ml-dashboard',   label: 'ML Models',        Icon: Brain           },
  { to: '/data-catalog',  label: 'Data Catalog',     Icon: Database        },
  { to: '/scenario',        label: 'Scenario Analysis', Icon: Activity       },
  { to: '/load-duration',  label: 'Load Statistics',  Icon: BarChart2      },
  { to: '/trends',         label: 'Historical Trends', Icon: Clock          },
  { to: '/frequency',      label: 'Frequency',         Icon: Radio          },
  { to: '/futures',        label: 'Energy Futures',    Icon: TrendingUp     },
  { to: '/registry',           label: 'Participants',        Icon: Building2      },
  { to: '/market-share-tracker', label: 'Market Share',      Icon: Users          },
  { to: '/outages',        label: 'Outage Schedule',   Icon: Wrench         },
  { to: '/der',            label: 'VPP & DER',         Icon: HomeIcon       },
  { to: '/gas',            label: 'Gas Market',        Icon: Flame          },
  { to: '/retail',         label: 'Retail Market',     Icon: Users          },
  { to: '/network',        label: 'Network & MLF',     Icon: Network        },
  { to: '/rez',            label: 'REZ & Infrastructure', Icon: Zap         },
  { to: '/curtailment',    label: 'Curtailment',       Icon: TrendingDown   },
  { to: '/dsp',            label: 'Demand Response',   Icon: Users          },
  { to: '/security',       label: 'System Security',   Icon: Shield         },
  { to: '/bidding',        label: 'Bidding Analytics', Icon: BarChart2      },
  { to: '/nem-events',     label: 'NEM Events',        Icon: Calendar       },
  { to: '/fcas-market',    label: 'FCAS Market',       Icon: Gauge          },
  { to: '/battery-econ',  label: 'Battery Economics', Icon: BatteryCharging },
  { to: '/settlement',    label: 'NEM Settlement',    Icon: FileText        },
  { to: '/carbon',        label: 'Carbon Analytics',  Icon: Leaf            },
  { to: '/hedging',       label: 'OTC Hedging',        Icon: TrendingUp     },
  { to: '/hydro',         label: 'Hydro Storage',      Icon: Droplets       },
  { to: '/market-power',  label: 'Market Power',       Icon: AlertTriangle  },
  { to: '/pasa',          label: 'PASA & Adequacy',    Icon: Activity       },
  { to: '/sra',           label: 'SRA Auctions',       Icon: ArrowRightLeft },
  { to: '/ppa',           label: 'PPA Market',         Icon: Sun            },
  { to: '/dispatch',      label: 'Dispatch Accuracy',  Icon: Target         },
  { to: '/regulatory',    label: 'Regulatory',         Icon: FileText       },
  { to: '/isp-tracker',   label: 'ISP Tracker',        Icon: MapPin         },
  { to: '/solar-ev',      label: 'Solar & EV',         Icon: Zap            },
  { to: '/lrmc',          label: 'LRMC & Investment',  Icon: TrendingDown   },
  { to: '/constraints',    label: 'Constraints',        Icon: GitBranch      },
  { to: '/price-setter',   label: 'Price Setter',       Icon: Target         },
  { to: '/tariff',         label: 'Tariff Analytics',   Icon: Receipt        },
  { to: '/grid-mod',       label: 'Grid Modernisation', Icon: Wifi           },
  { to: '/spot-cap',       label: 'Price Cap & CPT',   Icon: AlertTriangle  },
  { to: '/causer-pays',    label: 'Causer Pays',        Icon: Gauge          },
  { to: '/wem',            label: 'WEM Market',        Icon: Building2      },
  { to: '/inertia',         label: 'Inertia & Strength', Icon: Activity       },
  { to: '/tnsp',           label: 'TNSP & AER',        Icon: Network        },
  { to: '/surveillance',  label: 'Market Surveillance', Icon: Shield         },
  { to: '/hydrogen',       label: 'Green Hydrogen',    Icon: Flame          },
  { to: '/offshore-wind',          label: 'Offshore Wind',     Icon: Wind           },
  { to: '/offshore-wind-pipeline', label: 'Offshore Wind Pipeline', Icon: Waves       },
  { to: '/cer',          label: 'CER & RET',       Icon: Leaf           },
  { to: '/phes',          label: 'Pumped Hydro (PHES)', Icon: Droplets       },
  { to: '/safeguard',     label: 'Safeguard & ERF',   Icon: Leaf           },
  { to: '/transmission', label: 'Major Transmission', Icon: GitBranch },
  { to: '/dnsp',           label: 'DNSP Analytics',    Icon: Network        },
  { to: '/vpp',            label: 'VPP Performance',   Icon: BatteryCharging },
  { to: '/reform',         label: 'Market Reform',     Icon: FileText       },
  { to: '/tuos',          label: 'Network Pricing',   Icon: DollarSign     },
  { to: '/carbon-registry', label: 'Carbon Registry',  Icon: Leaf           },
  { to: '/ev',            label: 'EV Charging',       Icon: Zap            },
  { to: '/storage',       label: 'Storage Arbitrage', Icon: Battery        },
  { to: '/demand-forecast', label: 'Demand Forecast', Icon: TrendingUp    },
  { to: '/rez-development', label: 'REZ Development',   Icon: MapPin         },
  { to: '/congestion',     label: 'Congestion',        Icon: AlertOctagon   },
  { to: '/equity',          label: 'Energy Equity',     Icon: Heart          },
  { to: '/demand-response', label: 'Demand Response',  Icon: Activity       },
  { to: '/btm',             label: 'Behind-the-Meter', Icon: Sun            },
  { to: '/rab',             label: 'Network RAB',      Icon: Building2      },
  { to: '/realtime',        label: 'NEM Live',         Icon: Radio          },
  { to: '/rit',             label: 'Network RIT',      Icon: GitBranch      },
  { to: '/forward-curve',   label: 'Forward Curve',    Icon: TrendingUp     },
  { to: '/coal-retirement', label: 'Coal Retirement',  Icon: Flame          },
  { to: '/gas-gen',         label: 'Gas Economics',    Icon: Activity       },
  { to: '/consumer-protection', label: 'Consumer Protection', Icon: Shield   },
  { to: '/efor',            label: 'Generator EFOR',   Icon: Activity       },
  { to: '/climate-risk',    label: 'Climate Risk',     Icon: CloudLightning },
  { to: '/smart-grid',      label: 'Smart Grid',       Icon: Cpu            },
  { to: '/minimum-demand',  label: 'Min Demand',       Icon: TrendingDown   },
  { to: '/market-events',  label: 'Market Events',    Icon: AlertTriangle  },
  { to: '/battery-tech',   label: 'Battery Tech',     Icon: Battery        },
  { to: '/community-energy', label: 'Community Energy', Icon: Users         },
  { to: '/asset-management', label: 'Asset Management', Icon: Wrench        },
  { to: '/decarbonization', label: 'Net Zero Pathway', Icon: Leaf           },
  { to: '/nuclear-ldes',      label: 'Nuclear & LDES',     Icon: Atom           },
  { to: '/nuclear-energy',    label: 'Nuclear Energy',     Icon: Atom           },
  { to: '/nuclear-energy-economics', label: 'Nuclear Economics', Icon: Atom },
  { to: '/bidding-behaviour', label: 'Bidding Behaviour',  Icon: BarChart2      },
  { to: '/energy-poverty',    label: 'Energy Poverty',     Icon: Heart          },
  { to: '/spot-forecast',     label: 'Spot Forecast',      Icon: Zap            },
  { to: '/hydrogen-economy',  label: 'Hydrogen Economy',   Icon: Fuel           },
  { to: '/hydrogen-economy-analytics', label: 'H2 Economy Analytics', Icon: Atom },
  { to: '/carbon-credit',     label: 'Carbon Credits',     Icon: TreePine       },
  { to: '/grid-resilience',   label: 'Grid Resilience',    Icon: ShieldAlert    },
  { to: '/ev-fleet',          label: 'EV Fleet & V2G',     Icon: Car            },
  { to: '/rec-market',            label: 'REC Market (LGC/STC)',   Icon: Award        },
  { to: '/transmission-congestion', label: 'Transmission Congestion', Icon: Network    },
  { to: '/derms-orchestration', label: 'DERMS & VPP',          Icon: Layers         },
  { to: '/market-design',     label: 'Market Design',      Icon: BookOpen       },
  { to: '/rez-capacity',      label: 'REZ Capacity',       Icon: MapPin         },
  { to: '/retail-offer-comparison', label: 'Retail Offer Compare', Icon: Tag    },
  { to: '/system-operator',   label: 'System Operator',    Icon: AlertOctagon   },
  { to: '/network-tariff-reform', label: 'Network Tariff Reform', Icon: CircuitBoard },
  { to: '/spike-analysis',    label: 'Price Spike Analysis', Icon: Flame          },
  { to: '/storage-revenue-stack', label: 'Storage Revenue Stack', Icon: BarChart3  },
  { to: '/solar-resource',    label: 'Solar Resource',     Icon: SunMedium      },
  { to: '/futures-market-risk', label: 'Futures Market Risk', Icon: Activity     },
  { to: '/wind-resource',         label: 'Wind Resource',      Icon: Tornado        },
  { to: '/corporate-ppa-market', label: 'Corporate PPA Market', Icon: FileText  },
  { to: '/microgrid-raps',    label: 'Microgrids & RAPS',  Icon: Wifi           },
  { to: '/market-liquidity',  label: 'Market Liquidity',   Icon: BarChart       },
  { to: '/thermal-efficiency', label: 'Thermal Efficiency', Icon: Thermometer   },
  { to: '/industrial-demand-flex', label: 'Industrial Demand Flex', Icon: Factory },
  { to: '/storage-lca',       label: 'Storage LCA',        Icon: RefreshCw      },
  { to: '/interconnector-flow-analytics', label: 'Interconnector Flows', Icon: ArrowLeftRight },
  { to: '/isp-progress',                 label: 'ISP Progress',              Icon: MapIcon        },
  { to: '/firming-technology-economics', label: 'Firming Tech Economics',     Icon: Flame          },
  { to: '/demand-forecasting-models',   label: 'Demand Forecast Models',     Icon: Brain          },
  { to: '/market-stress-testing',       label: 'Market Stress Testing',      Icon: ShieldAlert    },
  { to: '/capacity-investment-signals', label: 'Capacity Investment Signals', Icon: TrendingUp     },
  { to: '/frequency-control-analytics', label: 'Frequency Control',          Icon: Activity       },
  { to: '/rec-certificate-tracking',   label: 'REC Certificate Tracking',    Icon: Award          },
  { to: '/spot-market-depth',          label: 'Spot Market Depth',            Icon: Layers         },
  { to: '/storage-tech-roadmap',       label: 'Storage Tech Roadmap',         Icon: GitBranch      },
  { to: '/renewable-integration-cost', label: 'Renewable Integration Cost',   Icon: GitMerge       },
  { to: '/planned-outage-analytics',  label: 'Planned Outage & Maintenance', Icon: Calendar       },
  { to: '/volatility-regime-analytics', label: 'Volatility Regime Analytics', Icon: TrendingUp    },
  { to: '/black-start-capability',      label: 'Black Start Capability',       Icon: ShieldAlert   },
  { to: '/ancillary-services-cost',     label: 'Ancillary Services Cost',      Icon: Gauge          },
  { to: '/cbam-trade-analytics',       label: 'CBAM Trade Analytics',         Icon: Globe          },
  { to: '/congestion-revenue-analytics', label: 'Congestion Revenue & SRA',  Icon: Network        },
  { to: '/climate-physical-risk',       label: 'Climate Physical Risk',      Icon: Thermometer    },
  { to: '/energy-affordability',        label: 'Energy Affordability',         Icon: DollarSign     },
  { to: '/electrification-analytics',  label: 'Building Electrification',     Icon: Flame          },
  { to: '/electricity-export-infra',   label: 'Electricity Export Infra',     Icon: Globe2         },
  { to: '/electricity-export-economics', label: 'Electricity Export Economics', Icon: Globe          },
  { to: '/ldes-economics',             label: 'LDES Economics',               Icon: Database       },
  { to: '/prosumer-analytics',         label: 'Prosumer & BTM Analytics',     Icon: Sun            },
  { to: '/gas-transition-analytics',  label: 'Gas Transition Analytics',     Icon: Fuel           },
  { to: '/tnsp-analytics',                  label: 'TNSP Revenue & Investment',    Icon: Building2      },
  { to: '/dnsp-analytics',                 label: 'DNSP Performance & Investment', Icon: Network        },
  { to: '/reliability-standard-analytics', label: 'NEM Reliability Standard',     Icon: ShieldCheck    },
  { to: '/storage-optimisation-analytics', label: 'Storage Revenue Optimisation', Icon: Battery },
  { to: '/settlement-analytics',           label: '5-Min Settlement & Prudential', Icon: CreditCard },
  { to: '/renewable-auction',             label: 'Renewable Auction Design & CfD Analytics', Icon: Award },
  { to: '/voll-analytics',               label: 'VoLL Outage Cost Analytics',   Icon: AlertTriangle },
  { to: '/demand-flexibility-analytics', label: 'Demand Flexibility & ILM',     Icon: Gauge      },
  { to: '/futures-price-discovery',      label: 'Futures Price Discovery',       Icon: TrendingUp },
  { to: '/electricity-price-index',     label: 'Electricity CPI & Retail',      Icon: ShoppingCart },
  { to: '/interconnector-upgrade-analytics', label: 'Interconnector Upgrade Business Case', Icon: ArrowLeftRight },
  { to: '/mlf-analytics',                   label: 'MLF Deep Dive Analytics',              Icon: TrendingDown   },
  { to: '/csp-analytics',                    label: 'CSP & Solar Thermal Analytics',         Icon: Sun            },
  { to: '/carbon-intensity-analytics',       label: 'Carbon Intensity Analytics',            Icon: Leaf           },
  { to: '/network-tariff-reform-analytics',  label: 'Tariff Reform & DER Incentives',         Icon: Sliders        },
  { to: '/tariff-cross-subsidy',             label: 'Cross-Subsidy & Cost-Reflective Tariffs', Icon: BarChart2      },
  { to: '/ai-digital-twin-analytics',        label: 'AI & Digital Twin Analytics',             Icon: Cpu            },
  { to: '/esoo-adequacy-analytics',          label: 'ESOO Generation Adequacy',                Icon: Activity       },
  { to: '/social-licence-analytics',        label: 'Social Licence & Equity Analytics',        Icon: Users          },
  { to: '/electricity-options',             label: 'Options Analytics',                         Icon: TrendingUp     },
  { to: '/grid-forming-inverter',           label: 'Grid-Forming Inverter & System Strength',    Icon: Zap            },
  { to: '/capacity-mechanism',              label: 'Capacity Mechanism',                          Icon: Shield         },
  { to: '/demand-forecast-accuracy', label: 'Forecast Accuracy', Icon: BarChart2 },
  { to: '/transmission-investment', label: 'Transmission Investment', Icon: Network },
  { to: '/rez-progress',      label: 'REZ Progress',       Icon: MapPin         },
  { to: '/storage-revenue',   label: 'Storage Revenue Analytics', Icon: BatteryCharging },
  { to: '/carbon-price-pathway', label: 'Carbon Price Pathway',    Icon: Leaf           },
  { to: '/spot-price-forecast', label: 'Spot Price Forecast Analytics', Icon: Brain },
  { to: '/ancillary-cost-allocation', label: 'Ancillary Cost Allocation', Icon: DollarSign },
  { to: '/wholesale-liquidity', label: 'Wholesale Market Liquidity', Icon: BarChart3 },
  { to: '/generator-retirement', label: 'Generator Retirement',   Icon: Power          },
  { to: '/consumer-hardship',    label: 'Consumer Hardship',       Icon: Heart          },
  { to: '/dsr-aggregator',       label: 'DSR Aggregator Analytics', Icon: Sliders        },
  { to: '/power-system-events',  label: 'Power System Security Events', Icon: AlertTriangle },
  { to: '/merchant-renewable',   label: 'Merchant Wind & Solar',        Icon: Wind           },
  { to: '/retailer-competition', label: 'Retailer Competition',          Icon: Users          },
  { to: '/storage-cost-curves', label: 'Storage Cost Curves',            Icon: TrendingDown   },
  { to: '/extreme-weather-resilience', label: 'Extreme Weather Resilience', Icon: Tornado        },
  { to: '/spot-price-volatility-regime', label: 'Spot Price Volatility Regime', Icon: Activity  },
  { to: '/industrial-electrification', label: 'Industrial Electrification',   Icon: Zap        },
  { to: '/industrial-electrification-x', label: 'Industrial Electrification X', Icon: Factory  },
  { to: '/offshore-wind-dev-analytics', label: 'Offshore Wind Dev Pipeline',  Icon: Wind       },
  { to: '/offshore-wind-finance',       label: 'Offshore Wind Finance',        Icon: Wind       },
  { to: '/carbon-offset-project',      label: 'Carbon Offset Projects',       Icon: Leaf       },
  { to: '/pumped-hydro-resource-assessment', label: 'Pumped Hydro Resource Assessment', Icon: Droplets },
  { to: '/frequency-control-performance', label: 'NEM Frequency Control Performance', Icon: Radio  },
  { to: '/cost-reflective-tariff-reform', label: 'Cost-Reflective Tariff Reform', Icon: BarChart2 },
  { to: '/ev-fleet-grid-impact',          label: 'EV Fleet Grid Impact',          Icon: Car          },
  { to: '/nem-market-microstructure',     label: 'NEM Market Microstructure',     Icon: TrendingUp   },
  { to: '/rooftop-solar-grid',           label: 'Rooftop Solar Grid Analytics',  Icon: Sun          },
  { to: '/rec-market-analytics',         label: 'REC Analytics (LGC & STC)',     Icon: Award        },
  { to: '/energy-poverty-analytics',    label: 'Energy Poverty Analytics',      Icon: Heart        },
  { to: '/hedge-effectiveness',         label: 'Hedge Effectiveness',           Icon: Shield        },
  { to: '/cbam-trade-exposure',         label: 'CBAM Trade Exposure',           Icon: Globe          },
  { to: '/demand-response-programs',   label: 'Demand Response Programs',     Icon: Sliders        },
  { to: '/interconnector-congestion',  label: 'Interconnector Congestion',    Icon: GitMerge       },
  { to: '/ppa-market',                label: 'PPA Market Analytics',         Icon: FileText       },
  { to: '/ppa-structuring',           label: 'PPA Structuring',              Icon: FileText       },
  { to: '/battery-dispatch-strategy', label: 'Battery Dispatch Strategy',    Icon: Battery        },
  { to: '/generation-mix-transition', label: 'Generation Mix Transition',    Icon: BarChart       },
  { to: '/storage-duration-economics', label: 'Storage Duration Economics',  Icon: Clock          },
  { to: '/ancillary-market-depth',    label: 'FCAS Market Depth Analytics', Icon: Layers         },
  { to: '/sra-analytics',            label: 'SRA Settlement Analytics',    Icon: DollarSign     },
  { to: '/spot-market-stress',       label: 'NEM Spot Market Stress Testing', Icon: AlertTriangle  },
  { to: '/electricity-workforce',   label: 'Electricity Workforce & Skills', Icon: Users          },
  { to: '/rez-transmission',        label: 'REZ Transmission Infrastructure', Icon: Network        },
  { to: '/network-regulatory-framework', label: 'Network Regulatory Framework', Icon: Scale         },
  { to: '/price-model-comparison',       label: 'Price Forecasting Model Comparison', Icon: Cpu    },
  { to: '/gas-electricity-nexus',        label: 'Gas-Electricity Nexus Analytics',     Icon: Flame         },
  { to: '/bidding-compliance',           label: 'Generator Bidding Compliance',        Icon: AlertOctagon  },
  { to: '/community-energy-analytics',  label: 'Community Energy & Microgrid',        Icon: Home          },
  { to: '/grid-cybersecurity',          label: 'Grid Cybersecurity & Resilience',     Icon: Lock          },
  { to: '/market-participant-financial', label: 'Market Participant Financial Health', Icon: CreditCard    },
  { to: '/digital-transformation',      label: 'Digital Transformation Analytics',   Icon: Smartphone    },
  { to: '/energy-transition-finance',   label: 'Energy Transition Finance',          Icon: DollarSign    },
  { to: '/negative-price-events',       label: 'NEM Negative Price Events',          Icon: TrendingDown  },
  { to: '/cer-orchestration',           label: 'CER Orchestration Analytics',        Icon: Wifi          },
  { to: '/system-load-balancing',       label: 'System Load Balancing & Reserve Adequacy', Icon: BarChart2 },
  { to: '/carbon-accounting',           label: 'Carbon Accounting & Scope 2 Analytics',    Icon: Leaf       },
  { to: '/wholesale-bidding-strategy',  label: 'NEM Wholesale Bidding Strategy Analytics',  Icon: TrendingUp },
  { to: '/ldes-analytics',             label: 'LDES Technology & Investment Analytics',     Icon: Database   },
  { to: '/emergency-management',        label: 'NEM Emergency Management & Contingency Response', Icon: AlertTriangle },
  { to: '/consumer-switching-retail-churn', label: 'Consumer Switching & Churn', Icon: Users },
  { to: '/solar-thermal-csp', label: 'Solar Thermal CSP', Icon: Sun },
  { to: '/nem-post-reform-market-design', label: 'Post-Reform Market Design', Icon: Scale },
  { to: '/electricity-price-forecasting-models', label: 'Price Forecasting Models', Icon: Activity },
  { to: '/large-industrial-demand', label: 'Large Industrial Demand', Icon: Factory },
  { to: '/network-investment-pipeline', label: 'Network Investment Pipeline', Icon: GitBranch },
  { to: '/nem-demand-forecast', label: 'NEM Demand Forecast', Icon: TrendingUp },
  { to: '/hydrogen-fuel-cell-vehicles', label: 'Hydrogen Fuel Cell Vehicles', Icon: Truck },
  { to: '/spot-price-spike-prediction', label: 'Spike Price Prediction', Icon: AlertOctagon },
  { to: '/grid-edge-technology', label: 'Grid Edge Technology', Icon: Cpu },
  { to: '/bess-degradation', label: 'Storage Degradation', Icon: Battery },
  { to: '/clean-hydrogen-production-cost', label: 'H2 Production Cost', Icon: Flame },
  { to: '/ancillary-services-procurement', label: 'AS Procurement', Icon: Radio },
  { to: '/rez-connection-queue', label: 'REZ Connection Queue', Icon: Grid },
  { to: '/australian-carbon-policy', label: 'Carbon Policy', Icon: Leaf },
  { to: '/market-design-simulation', label: 'Market Design Simulation', Icon: BarChart2 },
  { to: '/power-system-stability', label: 'Power System Stability', Icon: Zap },
  { to: '/energy-retail-competition', label: 'Retail Competition', Icon: ShoppingCart },
  { to: '/clean-energy-finance', label: 'Clean Energy Finance', Icon: DollarSign },
  { to: '/behind-meter-commercial', label: 'BTM Commercial Analytics', Icon: Building2 },
  { to: '/capacity-investment-scheme', label: 'CIS Analytics', Icon: Award },
  { to: '/demand-flexibility-market', label: 'Demand Flexibility Market', Icon: Sliders },
  { to: '/energy-asset-life-extension', label: 'Asset Life Extension', Icon: Wrench },
  { to: '/green-ammonia-export', label: 'Green Ammonia Export', Icon: Ship },
  { to: '/electricity-export-cable', label: 'Electricity Export Cable', Icon: Radio },
  { to: '/industrial-decarbonisation', label: 'Industrial Decarbonisation', Icon: Factory },
  { to: '/community-energy-storage', label: 'Community Energy Storage', Icon: Users },
  { to: '/nem-generation-mix', label: 'NEM Generation Mix', Icon: BarChart2 },
  { to: '/consumer-energy-affordability', label: 'Consumer Energy Affordability', Icon: Heart },
  { to: '/electricity-price-risk', label: 'Electricity Price Risk Mgmt', Icon: Shield },
  { to: '/ev-fleet-depot',    label: 'EV Fleet Depot Charging',    Icon: Truck  },
  { to: '/wind-farm-wake',    label: 'Wind Farm Wake Analytics',   Icon: Wind   },
  { to: '/market-bidding-strategy', label: 'Market Bidding Strategy Analytics', Icon: Target },
  { to: '/solar-pv-soiling',  label: 'Solar PV Soiling & Performance', Icon: Sun },
  { to: '/ot-ics-cyber-security', label: 'OT/ICS Energy Cyber Security', Icon: Lock },
  { to: '/stpasa-adequacy', label: 'STPASA Adequacy', Icon: AlertTriangle },
  { to: '/generator-performance-standards', label: 'Generator GPS Compliance', Icon: CheckCircle },
  { to: '/electricity-frequency-performance', label: 'Electricity Frequency Performance', Icon: Activity },
  { to: '/lgc-market',                        label: 'LGC Market Analytics',               Icon: Award    },
  { to: '/wave-tidal-ocean', label: 'Wave & Tidal Ocean Energy', Icon: Waves },
  { to: '/reactive-power-voltage', label: 'Reactive Power & Voltage', Icon: Zap },
  { to: '/battery-revenue-stack', label: 'Battery Revenue Stack', Icon: Battery },
  { to: '/digital-energy-twin', label: 'Digital Energy Twin', Icon: Monitor },
  { to: '/network-protection-system', label: 'Network Protection System', Icon: ShieldCheck },
  { to: '/pumped-hydro-dispatch', label: 'Pumped Hydro Dispatch', Icon: Droplets },
  { to: '/retail-market-design', label: 'Retail Market Design', Icon: ShoppingBag },
  { to: '/spot-market-depth-x', label: 'Spot Market Depth X', Icon: TrendingUp },
  { to: '/solar-farm-operations', label: 'Solar Farm O&M Analytics', Icon: Sun },
  { to: '/distribution-network-planning', label: 'Distribution Network Planning', Icon: Network },
  { to: '/grid-flexibility-services', label: 'Grid Flexibility Services Analytics', Icon: Moon },
  { to: '/hydrogen-refuelling-station', label: 'Hydrogen Refuelling Station Analytics', Icon: Fuel },
  { to: '/power-grid-climate-resilience', label: 'Power Grid Climate Resilience', Icon: CloudLightning },
  { to: '/energy-storage-tech-comparison', label: 'Storage Tech Comparison', Icon: Battery },
  { to: '/power-to-x-economics', label: 'Power-to-X Economics', Icon: Zap },
  { to: '/electricity-market-microstructure', label: 'Market Microstructure', Icon: BarChart2 },
  { to: '/grid-decarbonisation-pathway', label: 'Grid Decarbonisation', Icon: TrendingDown },
  { to: '/rooftop-solar-network-impact', label: 'Rooftop Solar Impact', Icon: Sun },
  { to: '/electricity-network-tariff-reform', label: 'Network Tariff Reform', Icon: DollarSign },
  { to: '/long-duration-energy-storage', label: 'Long Duration Storage', Icon: Database },
  { to: '/hydrogen-pipeline-infrastructure', label: 'H2 Pipeline Infrastructure', Icon: GitBranch },
  { to: '/carbon-capture-storage-project',  label: 'CCS Project Analytics',       Icon: Layers    },
  { to: '/energy-poverty-vulnerable-consumer', label: 'Energy Poverty',           Icon: Users     },
  { to: '/nuclear-small-modular-reactor',      label: 'SMR Nuclear Analytics',     Icon: Atom      },
  { to: '/electricity-market-transparency',    label: 'Market Transparency',        Icon: Eye       },
  { to: '/geothermal-energy-development',      label: 'Geothermal Development',     Icon: Thermometer },
  { to: '/solar-thermal-power-plant',          label: 'Solar Thermal CSP',          Icon: Sun         },
  { to: '/energy-trading-algorithmic-strategy', label: 'Algo Trading Strategy',     Icon: TrendingUp  },
  { to: '/ev-grid-integration-v2g',            label: 'EV Grid Integration V2G',   Icon: Plug        },
  { to: '/biomethane-gas-grid-injection',      label: 'Biomethane Gas Grid',        Icon: Leaf        },
  { to: '/electricity-market-forecasting-accuracy', label: 'Forecast Accuracy', Icon: Target      },
  { to: '/national-energy-transition-investment',  label: 'Energy Transition Investment', Icon: TrendingUp },
  { to: '/electricity-spot-price-seasonality', label: 'Price Seasonality', Icon: BarChart2 },
  { to: '/grid-congestion-constraint', label: 'Grid Congestion', Icon: GitBranch },
  { to: '/electricity-market-competition-concentration', label: 'Market Competition', Icon: PieChart },
  { to: '/renewable-energy-zone-development', label: 'REZ Development', Icon: MapIcon },
  { to: '/battery-storage-degradation-lifetime', label: 'Battery Degradation', Icon: Battery },
  { to: '/electricity-consumer-switching-churn', label: 'Consumer Switching', Icon: Users },
  { to: '/nem-inertia-synchronous-condenser', label: 'Inertia & Syncondensers', Icon: Activity },
  { to: '/offshore-wind-leasing-site', label: 'Offshore Wind Sites', Icon: Wind },
  { to: '/electricity-market-regulatory-appeals', label: 'Regulatory Appeals', Icon: Scale },
  { to: '/distributed-energy-resource-management', label: 'DER Management', Icon: Sliders },
  { to: '/market-price-formation-review', label: 'Price Formation Review', Icon: AlertCircle },
  { to: '/residential-solar-self-consumption', label: 'Solar Self-Consumption', Icon: HomeIcon },
  { to: '/energy-infrastructure-cyber-threat', label: 'Cyber Threat Analytics', Icon: Shield },
  { to: '/wholesale-gas-market', label: 'Wholesale Gas Market', Icon: Flame },
  { to: '/electricity-demand-forecasting-ml', label: 'Demand Forecast ML', Icon: Brain },
  { to: '/energy-storage-merchant-revenue', label: 'Storage Merchant Revenue', Icon: DollarSign },
  { to: '/transmission-access-reform', label: 'Transmission Access Reform', Icon: Network },
  { to: '/hydrogen-export-terminal', label: 'H2 Export Terminal', Icon: Ship },
  { to: '/grid-edge-technology-x', label: 'Grid Edge Technology X', Icon: Cpu },
  { to: '/energy-retailer-margin', label: 'Retailer Margin Analytics', Icon: ShoppingCart },
  { to: '/ev-fleet-charging', label: 'EV Fleet Charging Infra', Icon: Zap },
  { to: '/carbon-border-adjustment', label: 'Carbon Border Adjustment', Icon: Globe },
  { to: '/power-purchase-agreement-market', label: 'PPA Market Analytics', Icon: FileText },
  { to: '/distributed-solar-forecasting', label: 'Distributed Solar Forecasting', Icon: Sun },
  { to: '/electricity-market-liquidity', label: 'Market Liquidity Analytics', Icon: BarChart2 },
  { to: '/energy-transition-finance-x', label: 'Energy Transition Finance X', Icon: TrendingUp },
  { to: '/nem-frequency-control', label: 'NEM Frequency Control', Icon: Activity },
  { to: '/battery-second-life', label: 'Battery Second Life & Circular Economy', Icon: RefreshCw },
  { to: '/utility-solar-farm-operations', label: 'Utility Solar Farm Operations', Icon: Sun },
  { to: '/wind-farm-wake-effect', label: 'Wind Farm Wake Effect & Layout Optimisation', Icon: Wind },
  { to: '/energy-poverty-hardship', label: 'Energy Poverty & Hardship Analytics', Icon: Heart },
  { to: '/energy-poverty-hardship-x', label: 'Energy Hardship', Icon: Heart },
  { to: '/electricity-network-capital-investment', label: 'Electricity Network Capital Investment', Icon: Building },
  { to: '/gas-to-power-transition', label: 'Gas-to-Power Transition & Retirement', Icon: Flame },
  { to: '/carbon-offset-market', label: 'Australian Carbon Offset Market', Icon: Leaf },
  { to: '/power-system-stability-x', label: 'Power System Stability & Resilience', Icon: Shield },
  { to: '/aemo-market-operations', label: 'AEMO Market Operations & Dispatch', Icon: Radio },
  { to: '/renewable-energy-certificate', label: 'REC Market (LGC/STC) Analytics', Icon: Award },
  { to: '/energy-storage-dispatch-optimisation', label: 'Energy Storage Dispatch Optimisation', Icon: Battery },
  { to: '/offshore-wind-project-finance', label: 'Offshore Wind Project Finance', Icon: Anchor },
  { to: '/national-energy-market-reform', label: 'NEM Reform Impact Analytics', Icon: BookOpen },
  { to: '/electricity-market-price-formation', label: 'Electricity Market Price Formation', Icon: DollarSign },
  { to: '/rez-auction-cis', label: 'REZ Auction & CIS Analytics', Icon: MapPin },
  { to: '/grid-modernisation-digital-twin', label: 'Grid Modernisation & Digital Twin', Icon: Monitor },
  { to: '/energy-market-credit-risk', label: 'Energy Market Participant Credit Risk', Icon: AlertTriangle },
  { to: '/electricity-consumer-behaviour', label: 'Consumer Behaviour & Smart Home', Icon: HomeIcon },
  { to: '/thermal-coal-power-transition', label: 'Thermal Coal Power Station Transition', Icon: Zap },
  { to: '/demand-response-aggregator', label: 'Demand Response Aggregator Market', Icon: Users },
  { to: '/energy-commodity-trading', label: 'Energy Commodity Trading Desk', Icon: TrendingUp },
  { to: '/network-tariff-design-reform', label: 'Network Tariff Design & Reform', Icon: Tag },
  { to: '/hydrogen-valley-cluster', label: 'Hydrogen Valley & Industrial Cluster', Icon: Layers },
  { to: '/nem-congestion-rent', label: 'NEM Congestion Rent', Icon: Network },
  { to: '/electricity-retailer-churn', label: 'Retailer Churn', Icon: UserMinus },
  { to: '/energy-asset-maintenance', label: 'Asset Maintenance', Icon: Wrench },
  { to: '/coal-seam-gas', label: 'Coal Seam Gas', Icon: Flame },
  { to: '/ev-battery-technology', label: 'EV Battery Technology', Icon: Battery },
  { to: '/nem-demand-forecasting-accuracy', label: 'Demand Forecast Accuracy', Icon: Target },
  { to: '/power-system-inertia', label: 'System Inertia', Icon: Activity },
  { to: '/electricity-network-investment-deferral', label: 'Network Deferral', Icon: PauseCircle },
  { to: '/rez-capacity-factor', label: 'REZ Capacity Factors', Icon: Wind },
  { to: '/energy-retailer-hedging', label: 'Retailer Hedging', Icon: Shield },
  { to: '/gas-power-plant-flexibility', label: 'Gas Plant Flexibility', Icon: Flame },
  { to: '/solar-irradiance-resource', label: 'Solar Irradiance Resource', Icon: Sun },
  { to: '/electricity-price-cap-intervention', label: 'Price Cap Interventions', Icon: AlertTriangle },
  { to: '/biogas-landfill', label: 'Biogas & Landfill Gas', Icon: Leaf },
  { to: '/wind-resource-variability', label: 'Wind Resource Variability', Icon: Wind },
  { to: '/energy-storage-duration', label: 'Storage Duration', Icon: Clock },
  { to: '/nem-settlement-residue-auction', label: 'Settlement Residue Auction', Icon: Gavel },
  { to: '/hydrogen-electrolysis-cost', label: 'Hydrogen Electrolysis Cost', Icon: Zap },
  { to: '/electricity-demand-elasticity', label: 'Demand Elasticity', Icon: TrendingDown },
  { to: '/nuclear-energy-feasibility', label: 'Nuclear Feasibility', Icon: Atom },
  { to: '/transmission-congestion-revenue', label: 'Congestion Revenue', Icon: GitBranch },
  { to: '/electricity-market-design-reform', label: 'Market Design Reform', Icon: BookOpen },
  { to: '/carbon-capture-utilisation', label: 'Carbon Capture (CCUS)', Icon: Cloud },
  { to: '/grid-scale-battery-degradation', label: 'Battery Degradation', Icon: Battery },
  { to: '/electricity-export', label: 'Electricity Export', Icon: Globe },
  { to: '/dsm-programs',      label: 'DSM Programs',       Icon: Sliders        },
  { to: '/grid-topology',     label: 'Grid Topology',      Icon: GitBranch      },
  { to: '/rooftop-solar-fit', label: 'Rooftop Solar FiT',  Icon: Sun            },
  { to: '/lng-export',        label: 'LNG Export',         Icon: Ship           },
  { to: '/settings',          label: 'Settings',           Icon: SettingsIcon   },
  { to: '/community-microgrids', label: 'Community Microgrids', Icon: HomeIcon },
  { to: '/ewml-dashboard', label: 'Wholesale Liquidity', Icon: BarChart2 },
  { to: '/marine-energy',  label: 'Marine Energy',      Icon: Waves     },
  { to: '/geothermal-energy', label: 'Geothermal Energy', Icon: Thermometer },
  { to: '/storage-arbitrage', label: 'Storage Arbitrage', Icon: Zap },
  { to: '/carbon-border-adjustment-x', label: 'Carbon Border (CBAM)', Icon: Globe2 },
  { to: '/fcas-procurement', label: 'FCAS Procurement', Icon: Activity },
  { to: '/electricity-futures-options', label: 'Futures & Options', Icon: TrendingUp },
  { to: '/rec-lgc-stc', label: 'REC LGC/STC Market', Icon: Award },
  { to: '/der-management-x', label: 'DER Management', Icon: Grid },
  { to: '/coal-mine-energy', label: 'Coal Mine Energy', Icon: Layers },
  { to: '/nem-5min-settlement', label: '5-Min Settlement', Icon: Clock },
  { to: '/network-congestion-relief', label: 'Congestion Relief', Icon: AlertTriangle },
  { to: '/market-concentration-bidding', label: 'Market Concentration', Icon: PieChart },
  { to: '/industrial-energy-efficiency', label: 'Industrial Efficiency', Icon: Factory },
  { to: '/retailer-financial-health', label: 'Retailer Financial Health', Icon: DollarSign },
  { to: '/solar-farm-performance', label: 'Solar Farm Performance', Icon: Sun },
  { to: '/gas-network-pipeline', label: 'Gas Pipeline Network', Icon: GitBranch },
  { to: '/electricity-price-forecasting', label: 'Price Forecast Models', Icon: Target },
  { to: '/network-asset-life-cycle', label: 'Asset Life Cycle', Icon: SettingsIcon },
  { to: '/wind-farm-wake-turbine', label: 'Wind Farm Wake & Turbine', Icon: Wind },
  { to: '/hydrogen-refuelling-transport', label: 'H2 Transport', Icon: Truck },
  { to: '/electricity-spot-price-events', label: 'Price Events', Icon: Zap },
  { to: '/large-scale-renewable-auction', label: 'RE Auctions', Icon: Award },
  { to: '/nem-ancillary-services', label: 'Ancillary Services Reg', Icon: Shield },
  { to: '/australian-carbon-credit', label: 'ACCU Carbon Market', Icon: Leaf },
  { to: '/ev-grid-integration', label: 'EV Grid Integration', Icon: Car },
  { to: '/generator-capacity-adequacy', label: 'Capacity Adequacy', Icon: BarChart },
  { to: '/smart-grid-cybersecurity', label: 'Grid Cybersecurity', Icon: Lock },
  { to: '/pumped-hydro-reservoir', label: 'Pumped Hydro Ops', Icon: Droplets },
  { to: '/etjj-dashboard', label: 'Energy Transition Jobs', Icon: Briefcase },
  { to: '/nifr-dashboard', label: 'Interconnector Flow Rights', Icon: GitMerge },
  { to: '/cefa-dashboard-x', label: 'Clean Energy Finance', Icon: DollarSign },
  { to: '/bess-performance', label: 'BESS Performance', Icon: Battery },
  { to: '/elca-dashboard', label: 'Load Curve Analytics', Icon: Activity },
  { to: '/ngts-dashboard', label: 'Gas Trading & Settlement', Icon: Flame },
  { to: '/aeos-dashboard', label: 'Energy Optimisation', Icon: SettingsIcon },
  { to: '/aemc-rule-change', label: 'AEMC Rule Changes', Icon: BookOpen },
  { to: '/renx-dashboard', label: 'Renewable Export', Icon: Upload },
  { to: '/cppa-dashboard-x', label: 'Corporate PPA', Icon: FileText },
  { to: '/nzem-dashboard', label: 'Net Zero Emissions', Icon: Wind },
  { to: '/epsa-dashboard', label: 'Price Sensitivity', Icon: TrendingUp },
  { to: '/grpt-dashboard', label: 'Grid Reliability', Icon: Wifi },
  { to: '/mats-dashboard', label: 'Trading Strategy', Icon: BarChart2 },
  { to: '/emga-dashboard', label: 'Emerging Markets', Icon: Rocket },
  { to: '/epro-dashboard', label: 'Portfolio Risk Opt.', Icon: PieChart },
  { to: '/aehm-dashboard', label: 'Energy Hub Markets', Icon: Globe },
  { to: '/fmrp-dashboard', label: 'Frequency Reserve', Icon: Radio },
  { to: '/daro-dashboard', label: 'Distributed Assets', Icon: Cpu },
  { to: '/ecsa-dashboard', label: 'Consumer Segments', Icon: Users },
  { to: '/gena-dashboard', label: 'Generation Expansion', Icon: Zap },
  { to: '/nema-dashboard', label: 'Anomaly Detection', Icon: AlertTriangle },
  { to: '/wcms-dashboard', label: 'Wind Capacity Market', Icon: Wind },
  { to: '/spar-dashboard', label: 'Solar Park Registry', Icon: Sun },
  { to: '/energy-storage-duration-x', label: 'Storage Duration X', Icon: Battery },
  { to: '/offshore-wind-development', label: 'Offshore Wind', Icon: Wind },
  { to: '/nem-participant-financial', label: 'Participant Financial', Icon: TrendingUp },
  { to: '/green-tariff-hydrogen', label: 'Green Tariff & H2', Icon: Zap },
  { to: '/nem-price-review', label: 'Price Review', Icon: DollarSign },
  { to: '/renewable-certificate-nem', label: 'REC Analytics', Icon: Award },
  { to: '/demand-curve-price-anchor', label: 'Demand Curve', Icon: TrendingDown },
  { to: '/market-evolution-policy', label: 'Market Policy', Icon: Scale },
  { to: '/energy-grid-topology', label: 'Grid Topology', Icon: Network },
  { to: '/carbon-voluntary-exchange', label: 'Voluntary Carbon', Icon: Leaf },
  { to: '/renewable-market-sensitivity', label: 'RE Sensitivity', Icon: Sun },
  { to: '/natural-gas-pipeline', label: 'Gas Pipelines', Icon: GitMerge },
  { to: '/battery-chemistry-risk', label: 'Battery Risk', Icon: Shield },
  { to: '/aemo-5min-settlement', label: '5-Min Settlement', Icon: Clock },
  { to: '/retail-competition', label: 'Retail Competition', Icon: ShoppingCart },
  { to: '/demand-response-aggregation', label: 'Demand Response', Icon: Zap },
  { to: '/grid-frequency', label: 'Grid Frequency', Icon: Radio },
  { to: '/hydrogen-economy-outlook', label: 'Hydrogen Economy', Icon: Fuel },
  { to: '/network-augmentation', label: 'Network Deferral', Icon: GitBranch },
  { to: '/ev-fleet-grid', label: 'EV Grid Integration', Icon: Car },
  { to: '/grid-emissions-intensity', label: 'Grid Emissions', Icon: CloudRain },
  { to: '/microgrid-resilience', label: 'Microgrid Resilience', Icon: Shield },
  { to: '/power-quality', label: 'Power Quality', Icon: Gauge },
  { to: '/energy-poverty', label: 'Energy Poverty', Icon: Heart },
  { to: '/vpp-operations', label: 'VPP Operations', Icon: Network },
  { to: '/coal-retirement', label: 'Coal Retirement', Icon: Factory },
  { to: '/pumped-hydro', label: 'Pumped Hydro', Icon: Droplets },
  { to: '/smart-meter', label: 'Smart Meter Analytics', Icon: BarChart3 },
  { to: '/east-coast-gas', label: 'East Coast Gas', Icon: Flame },
  { to: '/isp-analytics', label: 'ISP Analytics', Icon: MapIcon },
  { to: '/community-battery', label: 'Community Battery', Icon: Battery },
  { to: '/energy-cyber-security', label: 'Cyber Security', Icon: Lock },
  { to: '/wholesale-market-reform', label: 'Wholesale Market Reform', Icon: Scale },
]

const ROUTE_MAP: Record<string, React.ComponentType> = {
  '/': Home,
  '/live': LiveMarket,
  '/forecasts': Forecasts,
  '/copilot': Copilot,
  '/genie': Genie,
  '/alerts': Alerts,
  '/monitoring': Monitoring,
  '/market-depth': MarketDepth,
  '/price-analysis': PriceAnalysis,
  '/interconnectors': Interconnectors,
  '/generator-fleet': GeneratorFleet,
  '/market-notices': MarketNotices,
  '/weather-demand': WeatherDemand,
  '/bess': BessAnalytics,
  '/trading-desk': TradingDesk,
  '/sustainability': Sustainability,
  '/merit-order': MeritOrder,
  '/ml-dashboard': MlDashboardPage,
  '/data-catalog': DataCatalog,
  '/scenario': ScenarioAnalysis,
  '/load-duration': LoadDuration,
  '/trends': HistoricalTrends,
  '/frequency': FrequencyAnalytics,
  '/futures': EnergyFutures,
  '/registry': ParticipantRegistry,
  '/outages': OutageSchedule,
  '/der': DerDashboard,
  '/gas': GasMarket,
  '/retail': RetailMarket,
  '/network': NetworkAnalytics,
  '/rez': RezInfrastructure,
  '/curtailment': CurtailmentAnalytics,
  '/dsp': DemandResponse,
  '/security': SystemSecurity,
  '/bidding': BiddingAnalytics,
  '/nem-events': NemEvents,
  '/fcas-market': FcasMarket,
  '/battery-econ': BatteryEconomics,
  '/settlement': NemSettlement,
  '/carbon': CarbonAnalytics,
  '/hedging': HedgingAnalytics,
  '/hydro': HydroStorage,
  '/market-power': MarketPower,
  '/pasa': PasaAnalytics,
  '/sra': SraAuction,
  '/ppa': PpaMarket,
  '/dispatch': DispatchAccuracy,
  '/regulatory': RegulatoryTracker,
  '/isp-tracker': IspTracker,
  '/solar-ev': SolarEvAnalytics,
  '/lrmc': LrmcAnalytics,
  '/constraints': NetworkConstraints,
  '/price-setter': PriceSetterAnalytics,
  '/tariff': TariffAnalytics,
  '/grid-mod': GridModernisation,
  '/spot-cap': SpotCapAnalytics,
  '/causer-pays': CauserPays,
  '/wem': WemOverview,
  '/inertia': InertiaAnalytics,
  '/tnsp': TnspAnalytics,
  '/surveillance': MarketSurveillance,
  '/hydrogen': HydrogenAnalytics,
  '/offshore-wind': OffshoreWind,
  '/offshore-wind-pipeline': OffshoreWindPipeline,
  '/cer': CerDashboard,
  '/phes': PhesAnalytics,
  '/safeguard': SafeguardAnalytics,
  '/transmission': TransmissionProjects,
  '/dnsp': DnspAnalytics,
  '/vpp': VppDashboard,
  '/reform': MarketReformTracker,
  '/tuos': TuosAnalytics,
  '/carbon-registry': CarbonRegistry,
  '/ev': EvCharging,
  '/storage': StorageArbitrage,
  '/demand-forecast': DemandForecastAnalytics,
  '/rez-development': RenewableEnergyZoneDevelopmentAnalytics,
  '/congestion': CongestionAnalytics,
  '/equity': EnergyEquity,
  '/demand-response': DemandResponseAnalytics,
  '/btm': BehindTheMeter,
  '/rab': RabAnalytics,
  '/realtime': NemRealTimeDashboard,
  '/rit': RitAnalytics,
  '/forward-curve': ForwardCurveAnalytics,
  '/coal-retirement': CoalRetirement,
  '/gas-gen': GasGenEconomics,
  '/consumer-protection': ConsumerProtection,
  '/efor': GeneratorAvailability,
  '/climate-risk': ClimateRiskAnalytics,
  '/smart-grid': SmartGridAnalytics,
  '/minimum-demand': MinimumDemandAnalytics,
  '/market-events': MarketEventsAnalysis,
  '/battery-tech': BatteryTechAnalytics,
  '/community-energy': CommunityEnergy,
  '/asset-management': AssetManagement,
  '/decarbonization': DecarbonizationPathway,
  '/nuclear-ldes': NuclearLongDuration,
  '/nuclear-energy': NuclearEnergyAnalytics,
  '/bidding-behaviour': BiddingBehaviour,
  '/energy-poverty': EnergyPoverty,
  '/spot-forecast': SpotForecastDashboard,
  '/hydrogen-economy': HydrogenEconomy,
  '/hydrogen-economy-analytics': HydrogenEconomyAnalytics,
  '/carbon-credit': CarbonCreditMarket,
  '/grid-resilience': GridResilience,
  '/ev-fleet': EvFleetCharging,
  '/rec-market': RecMarket,
  '/transmission-congestion': TransmissionCongestion,
  '/derms-orchestration': DermsOrchestration,
  '/market-design': MarketDesignReform,
  '/rez-capacity': RezCapacityTracking,
  '/retail-offer-comparison': RetailOfferComparison,
  '/system-operator': SystemOperatorActions,
  '/network-tariff-reform': NetworkTariffReform,
  '/spike-analysis': PriceSpikeAnalysis,
  '/storage-revenue-stack': StorageRevenueStack,
  '/solar-resource': SolarResourceAnalytics,
  '/futures-market-risk': FuturesMarketRisk,
  '/wind-resource': WindResourceAnalytics,
  '/corporate-ppa-market': CorporatePpaMarket,
  '/microgrid-raps': MicrogridRaps,
  '/market-liquidity': MarketLiquidity,
  '/thermal-efficiency': ThermalEfficiency,
  '/industrial-demand-flex': IndustrialDemandFlex,
  '/storage-lca': StorageLca,
  '/interconnector-flow-analytics': InterconnectorFlowAnalytics,
  '/isp-progress': IspProgressTracker,
  '/firming-technology-economics': FirmingTechnologyEconomics,
  '/demand-forecasting-models': DemandForecastingModels,
  '/market-stress-testing': MarketStressTesting,
  '/capacity-investment-signals': CapacityInvestmentSignals,
  '/frequency-control-analytics': FrequencyControlAnalytics,
  '/rec-certificate-tracking': RecCertificateTracking,
  '/spot-market-depth': SpotMarketDepthAnalytics,
  '/storage-tech-roadmap': StorageTechRoadmap,
  '/renewable-integration-cost': RenewableIntegrationCost,
  '/planned-outage-analytics': PlannedOutageAnalytics,
  '/market-share-tracker': MarketShareTracker,
  '/volatility-regime-analytics': VolatilityRegimeAnalytics,
  '/black-start-capability': BlackStartCapability,
  '/ancillary-services-cost': AncillaryServicesCost,
  '/cbam-trade-analytics': CbamTradeAnalytics,
  '/congestion-revenue-analytics': CongestionRevenueAnalytics,
  '/climate-physical-risk': ClimatePhysicalRisk,
  '/energy-affordability': EnergyAffordabilityAnalytics,
  '/electrification-analytics': ElectrificationAnalytics,
  '/electricity-export-infra': ElectricityExportInfra,
  '/electricity-export-economics': ElectricityExportEconomicsAnalytics,
  '/ldes-economics': LdesEconomicsAnalytics,
  '/gas-transition-analytics': GasTransitionAnalytics,
  '/prosumer-analytics': ProsumerAnalytics,
  '/tnsp-analytics': TnspAnalytics,
  '/dnsp-analytics': DnspPerformanceAnalytics,
  '/reliability-standard-analytics': ReliabilityStandardAnalytics,
  '/storage-optimisation-analytics': StorageOptimisationAnalytics,
  '/settlement-analytics': SettlementAnalytics,
  '/realtime-operations': RealtimeOperationsDashboard,
  '/renewable-auction': RenewableAuctionAnalytics,
  '/voll-analytics': VollAnalytics,
  '/demand-flexibility-analytics': DemandFlexibilityAnalytics,
  '/futures-price-discovery': FuturesPriceDiscovery,
  '/electricity-price-index': ElectricityPriceIndex,
  '/interconnector-upgrade-analytics': InterconnectorUpgradeAnalytics,
  '/mlf-analytics': MlfAnalytics,
  '/csp-analytics': CspAnalytics,
  '/carbon-intensity-analytics': CarbonIntensityAnalytics,
  '/network-tariff-reform-analytics': NetworkTariffReformAnalytics,
  '/tariff-cross-subsidy': TariffCrossSubsidyAnalytics,
  '/ai-digital-twin-analytics': AiDigitalTwinAnalytics,
  '/esoo-adequacy-analytics': EsooAdequacyAnalytics,
  '/social-licence-analytics': SocialLicenceAnalytics,
  '/electricity-options': ElectricityOptionsAnalytics,
  '/grid-forming-inverter': GridFormingInverterAnalytics,
  '/capacity-mechanism': CapacityMechanismAnalytics,
  '/demand-forecast-accuracy': DemandForecastAccuracyAnalytics,
  '/transmission-investment': TransmissionInvestmentAnalytics,
  '/rez-progress': RezProgressAnalytics,
  '/storage-revenue': StorageRevenueAnalytics,
  '/carbon-price-pathway': CarbonPricePathwayAnalytics,
  '/spot-price-forecast': SpotPriceForecastAnalytics,
  '/ancillary-cost-allocation': AncillaryCostAllocationAnalytics,
  '/wholesale-liquidity': MarketLiquidityAnalytics,
  '/generator-retirement': GeneratorRetirementAnalytics,
  '/consumer-hardship': ConsumerHardshipAnalytics,
  '/dsr-aggregator': DsrAggregatorAnalytics,
  '/power-system-events': PowerSystemEventsAnalytics,
  '/merchant-renewable': MerchantRenewableAnalytics,
  '/retailer-competition': RetailerCompetitionAnalytics,
  '/storage-cost-curves': StorageCostCurvesAnalytics,
  '/extreme-weather-resilience': ExtremeWeatherResilienceAnalytics,
  '/spot-price-volatility-regime': SpotPriceVolatilityRegimeAnalytics,
  '/industrial-electrification': IndustrialElectrificationAnalytics,
  '/offshore-wind-dev-analytics': OffshoreWindDevAnalytics,
  '/pumped-hydro-resource-assessment': PumpedHydroResourceAssessmentAnalytics,
  '/frequency-control-performance': FrequencyControlPerformanceAnalytics,
  '/cost-reflective-tariff-reform': CostReflectiveTariffReformAnalytics,
  '/ev-fleet-grid-impact': EVFleetGridImpactAnalytics,
  '/nem-market-microstructure': NEMMarketMicrostructureAnalytics,
  '/rooftop-solar-grid': RooftopSolarGridAnalytics,
  '/rec-market-analytics': RECMarketAnalytics,
  '/energy-poverty-analytics': EnergyPovertyAnalytics,
  '/hedge-effectiveness': HedgeEffectivenessAnalytics,
  '/cbam-trade-exposure': CBAMTradeExposureAnalytics,
  '/demand-response-programs': DemandResponseProgramAnalytics,
  '/interconnector-congestion': InterconnectorCongestionAnalytics,
  '/ppa-market': PPAMarketAnalytics,
  '/ppa-structuring': PPAStructuringAnalytics,
  '/battery-dispatch-strategy': BatteryDispatchStrategyAnalytics,
  '/generation-mix-transition': GenerationMixTransitionAnalytics,
  '/storage-duration-economics': StorageDurationEconomicsAnalytics,
  '/ancillary-market-depth': AncillaryServicesMarketDepthAnalytics,
  '/sra-analytics': SRAAnalyticsPage,
  '/spot-market-stress': SpotMarketStressAnalytics,
  '/electricity-workforce': ElectricityWorkforceAnalytics,
  '/rez-transmission': REZTransmissionAnalytics,
  '/network-regulatory-framework': NetworkRegulatoryFrameworkAnalytics,
  '/price-model-comparison': PriceModelComparisonAnalytics,
  '/gas-electricity-nexus': GasElectricityNexusAnalytics,
  '/bidding-compliance': BiddingComplianceAnalytics,
  '/community-energy-analytics': CommunityEnergyAnalytics,
  '/grid-cybersecurity': GridCybersecurityAnalytics,
  '/market-participant-financial': MarketParticipantFinancialAnalytics,
  '/digital-transformation': DigitalTransformationAnalytics,
  '/energy-transition-finance': EnergyTransitionFinanceAnalytics,
  '/negative-price-events': NegativePriceEventAnalytics,
  '/cer-orchestration': CEROrchestrationAnalytics,
  '/system-load-balancing': SystemLoadBalancingAnalytics,
  '/carbon-accounting': CarbonAccountingAnalytics,
  '/wholesale-bidding-strategy': WholesaleBiddingStrategyAnalytics,
  '/ldes-analytics': LDESAnalytics,
  '/emergency-management': EmergencyManagementAnalytics,
  '/consumer-switching-retail-churn': ConsumerSwitchingRetailChurnAnalytics,
  '/solar-thermal-csp': SolarThermalCSPAnalytics,
  '/nem-post-reform-market-design': NEMPostReformMarketDesignAnalytics,
  '/electricity-price-forecasting-models': ElectricityPriceForecastingModelAnalytics,
  '/large-industrial-demand': LargeIndustrialDemandAnalytics,
  '/network-investment-pipeline': NetworkInvestmentPipelineAnalytics,
  '/nem-demand-forecast': NEMDemandForecastAnalytics,
  '/hydrogen-fuel-cell-vehicles': HydrogenFuelCellVehicleAnalytics,
  '/spot-price-spike-prediction': SpotPriceSpikePredictionAnalytics,
  '/grid-edge-technology': GridEdgeTechnologyAnalytics,
  '/bess-degradation': EnergyStorageDegradationAnalytics,
  '/clean-hydrogen-production-cost': CleanHydrogenProductionCostAnalytics,
  '/ancillary-services-procurement': AncillaryServicesProcurementAnalytics,
  '/rez-connection-queue': REZConnectionQueueAnalytics,
  '/australian-carbon-policy': AustralianCarbonPolicyAnalytics,
  '/market-design-simulation': MarketDesignSimulationAnalytics,
  '/power-system-stability': PowerSystemStabilityAnalytics,
  '/energy-retail-competition': EnergyRetailCompetitionAnalytics,
  '/clean-energy-finance': CleanEnergyFinanceAnalytics,
  '/nuclear-energy-economics': NuclearEnergyEconomicsAnalytics,
  '/behind-meter-commercial': BehindMeterCommercialAnalytics,
  '/capacity-investment-scheme': CapacityInvestmentSchemeAnalytics,
  '/demand-flexibility-market': DemandFlexibilityMarketAnalytics,
  '/energy-asset-life-extension': EnergyAssetLifeExtensionAnalytics,
  '/green-ammonia-export': GreenAmmoniaExportAnalytics,
  '/electricity-export-cable': ElectricityExportCableAnalytics,
  '/industrial-decarbonisation': IndustrialDecarbonisationAnalytics,
  '/community-energy-storage': CommunityEnergyStorageAnalytics,
  '/nem-generation-mix': NEMGenerationMixAnalytics,
  '/consumer-energy-affordability': ConsumerEnergyAffordabilityAnalytics,
  '/electricity-price-risk': ElectricityPriceRiskAnalytics,
  '/ev-fleet-depot': EVFleetDepotAnalytics,
  '/wind-farm-wake': WindFarmWakeAnalytics,
  '/market-bidding-strategy': MarketBiddingStrategyAnalytics,
  '/solar-pv-soiling': SolarPVSoilingAnalytics,
  '/ot-ics-cyber-security': OTICSCyberSecurityAnalytics,
  '/stpasa-adequacy': STPASAAdequacyAnalytics,
  '/generator-performance-standards': GeneratorPerformanceStandardsAnalytics,
  '/biomass-bioenergy': BiomassBioenergyAnalytics,
  '/electricity-frequency-performance': ElectricityFrequencyPerformanceAnalytics,
  '/lgc-market': LGCMarketAnalytics,
  '/wave-tidal-ocean': WaveTidalOceanAnalytics,
  '/reactive-power-voltage': ReactivePowerVoltageAnalytics,
  '/battery-revenue-stack': BatteryRevenueStackAnalytics,
  '/digital-energy-twin': DigitalEnergyTwinAnalytics,
  '/network-protection-system': NetworkProtectionSystemAnalytics,
  '/pumped-hydro-dispatch': PumpedHydroDispatchAnalytics,
  '/retail-market-design': RetailMarketDesignAnalytics,
  '/spot-market-depth-x': SpotMarketDepthXAnalytics,
  '/solar-farm-operations': SolarFarmOperationsAnalytics,
  '/distribution-network-planning': DistributionNetworkPlanningAnalytics,
  '/grid-flexibility-services': GridFlexibilityServicesAnalytics,
  '/hydrogen-refuelling-station': HydrogenRefuellingStationAnalytics,
  '/offshore-wind-finance': OffshoreWindFinanceAnalytics,
  '/carbon-offset-project': CarbonOffsetProjectAnalytics,
  '/power-grid-climate-resilience': PowerGridClimateResilienceAnalytics,
  '/energy-storage-tech-comparison': EnergyStorageTechComparisonAnalytics,
  '/power-to-x-economics': PowerToXEconomicsAnalytics,
  '/electricity-market-microstructure': ElectricityMarketMicrostructureAnalytics,
  '/grid-decarbonisation-pathway': GridDecarbonisationPathwayAnalytics,
  '/rooftop-solar-network-impact': RooftopSolarNetworkImpactAnalytics,
  '/electricity-network-tariff-reform': ElectricityNetworkTariffReformAnalytics,
  '/long-duration-energy-storage': LongDurationEnergyStorageAnalytics,
  '/hydrogen-pipeline-infrastructure': HydrogenPipelineInfrastructureAnalytics,
  '/carbon-capture-storage-project': CarbonCaptureStorageProjectAnalytics,
  '/energy-poverty-vulnerable-consumer': EnergyPovertyVulnerableConsumerAnalytics,
  '/nuclear-small-modular-reactor': NuclearSmallModularReactorAnalytics,
  '/electricity-market-transparency': ElectricityMarketTransparencyAnalytics,
  '/geothermal-energy-development': GeothermalEnergyDevelopmentAnalytics,
  '/solar-thermal-power-plant': SolarThermalPowerPlantAnalytics,
  '/energy-trading-algorithmic-strategy': EnergyTradingAlgorithmicStrategyAnalytics,
  '/ev-grid-integration-v2g': EVGridIntegrationV2GAnalytics,
  '/biomethane-gas-grid-injection': BiomethaneGasGridInjectionAnalytics,
  '/electricity-market-forecasting-accuracy': ElectricityMarketForecastingAccuracyAnalytics,
  '/national-energy-transition-investment': NationalEnergyTransitionInvestmentAnalytics,
  '/electricity-spot-price-seasonality': ElectricitySpotPriceSeasonalityAnalytics,
  '/grid-congestion-constraint': GridCongestionConstraintAnalytics,
  '/electricity-market-competition-concentration': ElectricityMarketCompetitionConcentrationAnalytics,
  '/renewable-energy-zone-development': RenewableEnergyZoneDevelopmentAnalytics,
  '/battery-storage-degradation-lifetime': BatteryStorageDegradationLifetimeAnalytics,
  '/electricity-consumer-switching-churn': ElectricityConsumerSwitchingChurnAnalytics,
  '/nem-inertia-synchronous-condenser': NEMInertiaSynchronousCondenserAnalytics,
  '/offshore-wind-leasing-site': OffshoreWindLeasingSiteAnalytics,
  '/electricity-market-regulatory-appeals': ElectricityMarketRegulatoryAppealsAnalytics,
  '/distributed-energy-resource-management': DistributedEnergyResourceManagementAnalytics,
  '/market-price-formation-review': MarketPriceFormationReviewAnalytics,
  '/residential-solar-self-consumption': ResidentialSolarSelfConsumptionAnalytics,
  '/energy-infrastructure-cyber-threat': EnergyInfrastructureCyberThreatAnalytics,
  '/wholesale-gas-market': WholesaleGasMarketAnalytics,
  '/electricity-demand-forecasting-ml': ElectricityDemandForecastingMLAnalytics,
  '/energy-storage-merchant-revenue': EnergyStorageMerchantRevenueAnalytics,
  '/industrial-electrification-x': IndustrialElectrificationXAnalytics,
  '/transmission-access-reform': TransmissionAccessReformAnalytics,
  '/hydrogen-export-terminal': HydrogenExportTerminalAnalytics,
  '/grid-edge-technology-x': GridEdgeTechnologyXAnalytics,
  '/energy-retailer-margin': EnergyRetailerMarginAnalytics,
  '/ev-fleet-charging': EvFleetChargingAnalytics,
  '/carbon-border-adjustment': CarbonBorderAdjustmentAnalytics,
  '/power-purchase-agreement-market': PowerPurchaseAgreementMarketAnalytics,
  '/distributed-solar-forecasting': DistributedSolarForecastingAnalytics,
  '/electricity-market-liquidity': ElectricityMarketLiquidityAnalytics,
  '/energy-transition-finance-x': EnergyTransitionFinanceXAnalytics,
  '/nem-frequency-control': NemFrequencyControlAnalytics,
  '/battery-second-life': BatterySecondLifeAnalytics,
  '/utility-solar-farm-operations': UtilitySolarFarmOperationsAnalytics,
  '/wind-farm-wake-effect': WindFarmWakeEffectAnalytics,
  '/energy-poverty-hardship': EnergyPovertyHardshipAnalytics,
  '/electricity-network-capital-investment': ElectricityNetworkCapitalInvestmentAnalytics,
  '/gas-to-power-transition': GasToPowerTransitionAnalytics,
  '/carbon-offset-market': CarbonOffsetMarketAnalytics,
  '/power-system-stability-x': PowerSystemStabilityXAnalytics,
  '/aemo-market-operations': AemoMarketOperationsAnalytics,
  '/renewable-energy-certificate': RenewableEnergyCertificateAnalytics,
  '/energy-storage-dispatch-optimisation': EnergyStorageDispatchOptimisationAnalytics,
  '/offshore-wind-project-finance': OffshoreWindProjectFinanceAnalytics,
  '/national-energy-market-reform': NationalEnergyMarketReformAnalytics,
  '/electricity-market-price-formation': ElectricityMarketPriceFormationAnalytics,
  '/rez-auction-cis': RezAuctionCisAnalytics,
  '/grid-modernisation-digital-twin': GridModernisationDigitalTwinAnalytics,
  '/energy-market-credit-risk': EnergyMarketCreditRiskAnalytics,
  '/electricity-consumer-behaviour': ElectricityConsumerBehaviourAnalytics,
  '/thermal-coal-power-transition': ThermalCoalPowerTransitionAnalytics,
  '/demand-response-aggregator': DemandResponseAggregatorAnalytics,
  '/energy-commodity-trading': EnergyCommodityTradingAnalytics,
  '/network-tariff-design-reform': NetworkTariffDesignReformAnalytics,
  '/hydrogen-valley-cluster': HydrogenValleyClusterAnalytics,
  '/nem-congestion-rent': NemCongestionRentAnalytics,
  '/electricity-retailer-churn': ElectricityRetailerChurnAnalytics,
  '/energy-asset-maintenance': EnergyAssetMaintenanceAnalytics,
  '/coal-seam-gas': CoalSeamGasAnalytics,
  '/ev-battery-technology': EvBatteryTechnologyAnalytics,
  '/nem-demand-forecasting-accuracy': NemDemandForecastingAccuracyAnalytics,
  '/power-system-inertia': PowerSystemInertiaAnalytics,
  '/electricity-network-investment-deferral': ElectricityNetworkInvestmentDeferralAnalytics,
  '/rez-capacity-factor': RezCapacityFactorAnalytics,
  '/energy-retailer-hedging': EnergyRetailerHedgingAnalytics,
  '/gas-power-plant-flexibility': GasPowerPlantFlexibilityAnalytics,
  '/solar-irradiance-resource': SolarIrradianceResourceAnalytics,
  '/electricity-price-cap-intervention': ElectricityPriceCapInterventionAnalytics,
  '/biogas-landfill': BiogasLandfillAnalytics,
  '/wind-resource-variability': WindResourceVariabilityAnalytics,
  '/energy-storage-duration': EnergyStorageDurationAnalytics,
  '/nem-settlement-residue-auction': NemSettlementResidueAuctionAnalytics,
  '/hydrogen-electrolysis-cost': HydrogenElectrolysisCostAnalytics,
  '/electricity-demand-elasticity': ElectricityDemandElasticityAnalytics,
  '/nuclear-energy-feasibility': NuclearEnergyFeasibilityAnalytics,
  '/transmission-congestion-revenue': TransmissionCongestionRevenueAnalytics,
  '/electricity-market-design-reform': ElectricityMarketDesignReformAnalytics,
  '/carbon-capture-utilisation': CarbonCaptureUtilisationAnalytics,
  '/grid-scale-battery-degradation': GridScaleBatteryDegradationAnalytics,
  '/electricity-export': AustraliaElectricityExportAnalytics,
  '/dsm-programs': DemandSideManagementProgramAnalytics,
  '/grid-topology': PowerGridTopologyAnalytics,
  '/rooftop-solar-fit': RooftopSolarFeedInTariffAnalytics,
  '/lng-export': LngExportAnalytics,
  '/settings': Settings,
  '/community-microgrids': EnergyCommunityMicrogridAnalytics,
  '/ewml-dashboard': ElectricityWholesaleMarketLiquidityAnalytics,
  '/marine-energy': TidalWaveMarineEnergyAnalytics,
  '/geothermal-energy': GeothermalEnergyAnalytics,
  '/storage-arbitrage': EnergyStorageArbitrageAnalytics,
  '/carbon-border-adjustment-x': CarbonBorderAdjustmentXAnalytics,
  '/fcas-procurement': FcasProcurementAnalytics,
  '/electricity-futures-options': ElectricityFuturesOptionsAnalytics,
  '/rec-lgc-stc': RenewableEnergyCertificateXAnalytics,
  '/der-management-x': DistributedEnergyResourceManagementXAnalytics,
  '/coal-mine-energy': CoalMineEnergyAnalytics,
  '/nem-5min-settlement': NemFiveMinuteSettlementAnalytics,
  '/network-congestion-relief': NetworkCongestionReliefAnalytics,
  '/market-concentration-bidding': MarketConcentrationBiddingAnalytics,
  '/industrial-energy-efficiency': IndustrialEnergyEfficiencyAnalytics,
  '/retailer-financial-health': RetailerFinancialHealthAnalytics,
  '/solar-farm-performance': SolarFarmPerformanceAnalytics,
  '/gas-network-pipeline': GasNetworkPipelineAnalytics,
  '/electricity-price-forecasting': ElectricityPriceForecastingAnalytics,
  '/network-asset-life-cycle': NetworkAssetLifeCycleAnalytics,
  '/wind-farm-wake-turbine': WindFarmWakeTurbineAnalytics,
  '/energy-poverty-hardship-x': EnergyPovertyHardshipXAnalytics,
  '/hydrogen-refuelling-transport': HydrogenRefuellingTransportAnalytics,
  '/electricity-spot-price-events': ElectricitySpotPriceEventAnalytics,
  '/large-scale-renewable-auction': LargeScaleRenewableAuctionAnalytics,
  '/nem-ancillary-services': NemAncillaryServicesAnalytics,
  '/australian-carbon-credit': AustralianCarbonCreditAnalytics,
  '/ev-grid-integration': ElectricVehicleGridIntegrationAnalytics,
  '/generator-capacity-adequacy': GeneratorCapacityAdequacyAnalytics,
  '/smart-grid-cybersecurity': SmartGridCybersecurityAnalytics,
  '/pumped-hydro-reservoir': PumpedHydroReservoirAnalytics,
  '/etjj-dashboard': EnergyTransitionJobsAnalytics,
  '/nifr-dashboard': InterconnectorFlowRightsAnalytics,
  '/cefa-dashboard-x': CleanEnergyFinanceXAnalytics,
  '/bess-performance': BessPerformanceAnalytics,
  '/elca-dashboard': LoadCurveAnalytics,
  '/ngts-dashboard': NaturalGasTradingAnalytics,
  '/aeos-dashboard': EnergyOptimisationAnalytics,
  '/aemc-rule-change': AemcRuleChangeAnalytics,
  '/renx-dashboard': RenewableExportAnalytics,
  '/cppa-dashboard-x': CorporatePpaAnalytics,
  '/nzem-dashboard': NetZeroEmissionsAnalytics,
  '/epsa-dashboard': PriceSensitivityAnalytics,
  '/grpt-dashboard': GridReliabilityAnalytics,
  '/mats-dashboard': MarketTradingStrategyAnalytics,
  '/emga-dashboard': EmergingMarketsAnalytics,
  '/epro-dashboard': PortfolioRiskOptimisationAnalytics,
  '/aehm-dashboard': EnergyHubMicrostructureAnalytics,
  '/fmrp-dashboard': FrequencyReservePlanningAnalytics,
  '/daro-dashboard': DistributedAssetOptimisationAnalytics,
  '/ecsa-dashboard': ConsumerSegmentationAnalytics,
  '/gena-dashboard': GenerationExpansionAnalytics,
  '/nema-dashboard': MarketAnomalyDetectionAnalytics,
  '/wcms-dashboard': WindCapacityMarketAnalytics,
  '/spar-dashboard': SolarParkRegistryAnalytics,
  '/energy-storage-duration-x': EnergyStorageDurationXAnalytics,
  '/offshore-wind-development': OffshoreWindDevelopmentAnalytics,
  '/nem-participant-financial': NemParticipantFinancialAnalytics,
  '/green-tariff-hydrogen': GreenTariffHydrogenAnalytics,
  '/nem-price-review': NemPriceReviewAnalytics,
  '/renewable-certificate-nem': RenewableCertificateNemAnalytics,
  '/demand-curve-price-anchor': DemandCurvePriceAnchorAnalytics,
  '/market-evolution-policy': MarketEvolutionPolicyAnalytics,
  '/energy-grid-topology': EnergyGridTopologyAnalytics,
  '/carbon-voluntary-exchange': CarbonVoluntaryExchangeAnalytics,
  '/renewable-market-sensitivity': RenewableMarketSensitivityAnalytics,
  '/natural-gas-pipeline': NaturalGasPipelineAnalytics,
  '/battery-chemistry-risk': BatteryChemistryRiskAnalytics,
  '/aemo-5min-settlement': Aemo5MinSettlementAnalytics,
  '/retail-competition': ElectricityRetailCompetitionAnalytics,
  '/demand-response-aggregation': DemandResponseAggregationAnalytics,
  '/grid-frequency': GridFrequencyResponseAnalytics,
  '/hydrogen-economy-outlook': HydrogenEconomyOutlookAnalytics,
  '/network-augmentation': NetworkAugmentationDeferralAnalytics,
  '/ev-fleet-grid': EvFleetGridIntegrationAnalytics,
  '/grid-emissions-intensity': GridEmissionsIntensityAnalytics,
  '/microgrid-resilience': MicrogridResilienceAnalytics,
  '/power-quality': PowerQualityMonitoringAnalytics,
  '/vpp-operations': VirtualPowerPlantOperationsAnalytics,
  '/pumped-hydro': PumpedHydroStorageAnalytics,
  '/smart-meter': SmartMeterDataAnalytics,
  '/east-coast-gas': EastCoastGasMarketAnalytics,
  '/isp-analytics': IntegratedSystemPlanAnalytics,
  '/community-battery': CommunityBatteryAnalytics,
  '/energy-cyber-security': EnergySectorCyberSecurityAnalytics,
  '/wholesale-market-reform': WholesaleMarketReformAnalytics,
}

// ---------------------------------------------------------------------------
// Sidebar — Flat category navigation
// ---------------------------------------------------------------------------

const PINNED_PATHS = new Set(['/', '/live', '/copilot', '/genie', '/alerts'])

const GROUP_DEFS: { key: string; label: string; Icon: React.ComponentType<{ size?: number; className?: string }> }[] = [
  { key: 'operations',  label: 'Market Operations',   Icon: Radio },
  { key: 'prices',      label: 'Prices & Trading',    Icon: DollarSign },
  { key: 'generation',  label: 'Generation',           Icon: Zap },
  { key: 'renewables',  label: 'Renewables',           Icon: Sun },
  { key: 'storage',     label: 'Energy Storage',       Icon: Battery },
  { key: 'network',     label: 'Network & Grid',       Icon: Network },
  { key: 'demand',      label: 'Demand & Weather',     Icon: Thermometer },
  { key: 'system',      label: 'FCAS & Security',      Icon: Shield },
  { key: 'der',         label: 'DER, EV & Smart Grid', Icon: HomeIcon },
  { key: 'gas',         label: 'Gas Market',            Icon: Flame },
  { key: 'emerging',    label: 'Hydrogen & Nuclear',    Icon: Atom },
  { key: 'climate',     label: 'Carbon & Climate',      Icon: Leaf },
  { key: 'retail',      label: 'Retail & Consumer',     Icon: Users },
  { key: 'policy',      label: 'Policy & Regulation',   Icon: FileText },
  { key: 'analytics',   label: 'Analytics & AI',        Icon: Brain },
  { key: 'other',       label: 'More',                  Icon: Layers },
]

/** Classify a nav item into a group key based on path and label keywords. */
function classifyNavItem(to: string, label: string): string {
  // Exact path overrides for short/ambiguous paths
  const exact: Record<string, string> = {
    '/monitoring': 'operations', '/settings': 'retail',
    '/ml-dashboard': 'analytics', '/data-catalog': 'analytics',
    '/scenario': 'analytics', '/trends': 'analytics',
    '/gas': 'gas', '/retail': 'retail', '/security': 'system',
    '/dsp': 'demand', '/der': 'der', '/curtailment': 'renewables',
    '/outages': 'generation', '/load-duration': 'demand',
    '/weather-demand': 'demand', '/sustainability': 'renewables',
    '/frequency': 'system', '/pasa': 'system',
  }
  if (exact[to]) return exact[to]

  const s = (to + ' ' + label).toLowerCase()

  if (/realtime|live.ops|market.notice|nem.event|surveillance|aemo.*operation|emergency|system.operator|stpasa|market.*transparen|market.*event/.test(s)) return 'operations'
  if (/price|spike|volatil|forecast(?!.*model)|spot(?!.*solar)|forward.curve|futures|trading|merit|hedg|bidding|settle|sra|voll|market.depth|market.liquid|wholesale.*liquid|market.stress|market.*micro|market.share|market.power|market.concent|credit.risk|participant.fin|algo.trad|commodity.trad|wholesale.*bidd|electricity.*option|ewml|epsa|mats|negative.*price|congestion.re[vn]/.test(s)) return 'prices'
  if (/generator|efor|coal.*retire|gas.*econ|gas.*gen(?!.*grid)|gas.*flex|thermal.eff|dispatch.acc|generation.*mix|generation.*expan|gena|capacity.*adequ|outage|planned.*outage/.test(s)) return 'generation'
  if (/batter|bess|storage|pumped.hydro|phes|ldes|long.duration/.test(s)) return 'storage'
  if (/solar(?!.*ev)|wind|offshore|renewab|ppa|rec.market|rec.lgc|rec.certif|lgc.*market|merchant.wind|cer[^s]|clean.energy|biomass|bioenergy|sustain|curtail|cppa|renx|rooftop.*solar(?!.*grid)/.test(s)) return 'renewables'
  if (/interconnect|network|constrain|congestion(?!.*rev)|grid.mod|tnsp|dnsp|rez|transmiss|mlf|distribut.*network|grid.topology|rab.analyt|rit.analyt/.test(s)) return 'network'
  if (/fcas|frequen|system.security|causer|inertia|pasa|stability|black.start|reactive|power.quality|ancillar|system.strength|grid.freq|fmrp/.test(s)) return 'system'
  if (/demand|weather|load.stat|load.dur|load.curve|minimum.demand|large.industrial|demand.elastic|demand.curve|elca|nem.demand|dsm|dsr/.test(s)) return 'demand'
  if (/der|vpp|ev.fleet|ev.charg|ev.grid|v2g|smart.grid|smart.meter|microgrid|rooftop.*grid|behind.*meter|prosumer|community.energ|community.batter|community.micro|derms|grid.edge|digital.*twin|daro|btm/.test(s)) return 'der'
  if (/hydrogen|nuclear|geotherm|wave.*tidal|ocean|power.to.x|ammonia|biomethane|fuel.cell|marine.energy|nzem/.test(s)) return 'emerging'
  if (/retail|consumer|tariff(?!.*reform)|energy.pover|affordab|hardship|switching|churn|ecsa|electricity.cpi|smart.home|settings/.test(s)) return 'retail'
  if (/carbon|climate|decarbon|net.zero|safeguard|emission|social.licence|equity|grid.resilien|extreme.weather|cbam/.test(s)) return 'climate'
  if (/gas.market|gas.network|gas.pipeline|gas.trading|lng|coal.seam|east.coast.gas|gas.transition|gas.electric|ngts|wholesale.gas/.test(s)) return 'gas'
  if (/regulat|isp|reform|aemc|rule.change|market.design|capacity.mechan|cis.analyt|workforce|policy/.test(s)) return 'policy'
  if (/ml.model|data.catalog|scenario|histor|trend|anomaly.detect|nema|forecast.*model/.test(s)) return 'analytics'
  return 'other'
}

const CATEGORY_SUBGROUPS: Record<string, { key: string; label: string; pattern: RegExp }[]> = {
  operations: [
    { key: 'realtime', label: 'Real-Time Ops', pattern: /realtime|live.ops|nem.live|5.?min/ },
    { key: 'notices', label: 'Notices & Events', pattern: /notice|event|surveillance|emergency/ },
    { key: 'dispatch', label: 'Dispatch & Settlement', pattern: /dispatch|settlement|aemo.*oper|system.operator/ },
    { key: 'monitoring', label: 'Monitoring', pattern: /monitor|transparen|stpasa/ },
  ],
  prices: [
    { key: 'spot', label: 'Spot Prices', pattern: /spot|price.analy|price.set|price.cap|negative.price|price.index|seasonal|price.event|price.form|price.review|price.anchor/ },
    { key: 'futures', label: 'Futures & Forward', pattern: /future|forward|option|discover/ },
    { key: 'trading', label: 'Trading', pattern: /trading|commodity|algo|merit|bidding|wholesale.*bidd|desk/ },
    { key: 'hedging', label: 'Hedging & Risk', pattern: /hedg|otc|risk|stress|voll|credit|participant.*fin|market.power|concentration/ },
    { key: 'forecasting', label: 'Forecasting', pattern: /forecast|predict|spike.*analy|volatil|model.comp/ },
    { key: 'settlement', label: 'Settlement', pattern: /settl|sra|congestion.re[vn]/ },
    { key: 'structure', label: 'Market Structure', pattern: /depth|liquid|share|micro|market.evolution/ },
  ],
  generation: [
    { key: 'fleet', label: 'Generator Fleet', pattern: /generator|fleet|efor|availab|perform|capacity.*adequ/ },
    { key: 'coal', label: 'Coal & Thermal', pattern: /coal|thermal|retire/ },
    { key: 'gas-gen', label: 'Gas Generation', pattern: /gas.*gen|gas.*econ|gas.*flex|gas.*power/ },
    { key: 'dispatch', label: 'Dispatch & Mix', pattern: /dispatch|generation.*mix|expansion|outage|planned/ },
  ],
  renewables: [
    { key: 'solar', label: 'Solar', pattern: /solar(?!.*ev)|irradianc|soiling|csp|rooftop(?!.*grid)/ },
    { key: 'wind', label: 'Wind', pattern: /wind|wake/ },
    { key: 'offshore', label: 'Offshore Wind', pattern: /offshore/ },
    { key: 'ppa', label: 'PPA & Certificates', pattern: /ppa|rec|lgc|stc|certif|cer|merchant|renew.*auction/ },
    { key: 'integration', label: 'Integration', pattern: /curtail|integrat|sustain|biomass|bioenergy/ },
  ],
  storage: [
    { key: 'bess', label: 'Battery (BESS)', pattern: /batter|bess/ },
    { key: 'phes', label: 'Pumped Hydro', pattern: /pump|phes|hydro/ },
    { key: 'ldes', label: 'Long Duration', pattern: /ldes|long.dur|duration/ },
    { key: 'economics', label: 'Economics', pattern: /econ|cost|revenue|arbitrage|merchant|lca|optimis/ },
    { key: 'tech', label: 'Technology', pattern: /tech|roadmap|degrad|second.life|chemist|comparison|community.*batter/ },
  ],
  network: [
    { key: 'transmission', label: 'Transmission', pattern: /transmiss|tnsp|interconnect|congestion|flow/ },
    { key: 'distribution', label: 'Distribution', pattern: /dnsp|distribut|augment|protection/ },
    { key: 'rez', label: 'REZ & Planning', pattern: /rez|renewable.*energy.*zone|connection.*queue/ },
    { key: 'pricing', label: 'Network Pricing', pattern: /mlf|tariff|tuos|rab|rit|investment|asset.*life/ },
    { key: 'grid', label: 'Grid & Topology', pattern: /grid.mod|topology|modernis|cyber/ },
  ],
  demand: [
    { key: 'forecast', label: 'Demand Forecast', pattern: /forecast|ml|accuracy/ },
    { key: 'load', label: 'Load & Statistics', pattern: /load|statist|duration|curve|minimum|weather/ },
    { key: 'response', label: 'Demand Response', pattern: /response|flex|dsm|dsr|aggregat|industrial|elasticit|large/ },
  ],
  system: [
    { key: 'fcas', label: 'FCAS Market', pattern: /fcas|ancillar|procure|cost.alloc|service/ },
    { key: 'frequency', label: 'Frequency', pattern: /frequen|causer|control|reserve/ },
    { key: 'stability', label: 'System Stability', pattern: /inertia|stability|black.start|reactive|power.quality|synchron|grid.form|strength|load.balanc|reliab/ },
    { key: 'security', label: 'Security', pattern: /security|pasa|adequ|esoo/ },
  ],
  der: [
    { key: 'vpp', label: 'VPP & DERMS', pattern: /vpp|derms|orchestrat|virtual|distributed.*asset/ },
    { key: 'ev', label: 'EV & V2G', pattern: /ev|v2g|vehicle|charging|depot|fleet.*grid/ },
    { key: 'solar-der', label: 'Rooftop & BTM', pattern: /rooftop|behind|btm|prosumer|self.consum|feed.in|solar.*grid/ },
    { key: 'smart', label: 'Smart Grid', pattern: /smart|meter|microgrid|community|digital.*twin|grid.edge|cyber/ },
  ],
  gas: [
    { key: 'market', label: 'Gas Market', pattern: /gas.market|wholesale.*gas|east.coast|trading/ },
    { key: 'network', label: 'Gas Network', pattern: /pipeline|network|lng|export/ },
    { key: 'transition', label: 'Gas Transition', pattern: /transition|coal.seam|nexus|biomethane|biogas/ },
  ],
  emerging: [
    { key: 'hydrogen', label: 'Hydrogen', pattern: /hydrogen|electroly|refuell|valley|h2|green.tariff/ },
    { key: 'nuclear', label: 'Nuclear', pattern: /nuclear|smr|feasib/ },
    { key: 'other-emerging', label: 'Other', pattern: /geotherm|wave|tidal|ocean|marine|power.to.x|ammonia|fuel.cell/ },
  ],
  climate: [
    { key: 'carbon', label: 'Carbon & Emissions', pattern: /carbon|emission|offset|credit|voluntary|capture|accounting|intensity|safeguard/ },
    { key: 'policy', label: 'Climate Policy', pattern: /cbam|border|net.zero|decarbon|pathway|policy/ },
    { key: 'risk', label: 'Climate Risk', pattern: /climate|resilien|extreme|physical|equity|social|afford/ },
  ],
  retail: [
    { key: 'market', label: 'Retail Market', pattern: /retail.*market|competition|offer|comparison|design|margin|financial/ },
    { key: 'consumer', label: 'Consumer', pattern: /consumer|poverty|hardship|afford|switching|churn|segment|behaviour/ },
    { key: 'tariff', label: 'Tariffs', pattern: /tariff|cross.subsid|cost.reflect/ },
  ],
  policy: [
    { key: 'regulation', label: 'Regulation', pattern: /regulat|aemc|rule|appeal/ },
    { key: 'market-design', label: 'Market Design', pattern: /market.design|reform|capacity.mechan|cis|post.reform|simulation|evolution/ },
    { key: 'planning', label: 'Planning', pattern: /isp|workforce|transition.*invest|finance|job/ },
  ],
  analytics: [
    { key: 'ml', label: 'ML & AI', pattern: /ml|model|anomaly|digital.*twin|optimis/ },
    { key: 'data', label: 'Data & Catalog', pattern: /data|catalog|scenario|histor|trend/ },
    { key: 'forecasting', label: 'Forecasting', pattern: /forecast/ },
  ],
}

function subClassifyNavItem(categoryKey: string, to: string, label: string): string {
  const subGroups = CATEGORY_SUBGROUPS[categoryKey]
  if (!subGroups) return 'other'
  const s = (to + ' ' + label).toLowerCase()
  for (const sg of subGroups) {
    if (sg.pattern.test(s)) return sg.key
  }
  return 'other'
}

function CategoryPage() {
  const { groupKey } = useParams<{ groupKey: string }>()
  const [activePath, setActivePath] = useState<string | null>(null)
  const [openMenu, setOpenMenu] = useState<string | null>(null)
  const menuBarRef = useRef<HTMLDivElement>(null)

  const group = useMemo(() => {
    const def = GROUP_DEFS.find(g => g.key === groupKey)
    if (!def) return null
    const items = NAV_ITEMS.filter(
      item => !PINNED_PATHS.has(item.to) && item.to !== '/settings' && classifyNavItem(item.to, item.label) === groupKey
    )
    return { ...def, items }
  }, [groupKey])

  // Build sub-grouped menus
  const menus = useMemo(() => {
    if (!group || !groupKey) return []
    const subDefs = CATEGORY_SUBGROUPS[groupKey] ?? []
    const grouped = new Map<string, typeof NAV_ITEMS>()

    for (const item of group.items) {
      const sk = subClassifyNavItem(groupKey, item.to, item.label)
      if (!grouped.has(sk)) grouped.set(sk, [])
      grouped.get(sk)!.push(item)
    }

    const result = subDefs
      .filter(sd => grouped.has(sd.key) && grouped.get(sd.key)!.length > 0)
      .map(sd => ({ ...sd, items: grouped.get(sd.key)! }))

    // Add "Other" bucket for unclassified items
    const otherItems = grouped.get('other')
    if (otherItems && otherItems.length > 0) {
      result.push({ key: 'other', label: 'More', pattern: /.*/, items: otherItems })
    }

    return result
  }, [group, groupKey])

  // Reset when group changes
  useEffect(() => {
    setActivePath(null)
    setOpenMenu(null)
  }, [groupKey])

  // Set first item as default active
  useEffect(() => {
    if (!activePath && menus.length > 0 && menus[0].items.length > 0) {
      setActivePath(menus[0].items[0].to)
    }
  }, [menus, activePath])

  // Close dropdown on outside click
  useEffect(() => {
    if (!openMenu) return
    const handler = (e: MouseEvent) => {
      if (menuBarRef.current && !menuBarRef.current.contains(e.target as Node)) {
        setOpenMenu(null)
      }
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [openMenu])

  if (!group || group.items.length === 0) {
    return (
      <div className="flex items-center justify-center h-full text-gray-400 text-sm">
        Category not found
      </div>
    )
  }

  const ActiveComponent = activePath ? ROUTE_MAP[activePath] : null
  const activeLabel = group.items.find(i => i.to === activePath)?.label

  return (
    <div className="flex flex-col h-full">
      {/* Menu bar */}
      <div className="sticky top-0 z-20 bg-white dark:bg-[#161B22] border-b border-gray-200 dark:border-[#30363D] shrink-0">
        {/* Category title row */}
        <div className="flex items-center gap-2 px-5 pt-3 pb-1">
          <group.Icon size={16} className="text-gray-400" />
          <h2 className="text-sm font-semibold text-gray-800 dark:text-gray-100">{group.label}</h2>
          {activeLabel && (
            <span className="text-xs text-gray-400 ml-1">/ {activeLabel}</span>
          )}
        </div>
        {/* Menu items row */}
        <div ref={menuBarRef} className="relative flex items-center px-4 pb-0">
          {menus.map(menu => {
            const isOpen = openMenu === menu.key
            const hasActive = menu.items.some(i => i.to === activePath)
            return (
              <div key={menu.key} className="relative">
                <button
                  onClick={() => setOpenMenu(isOpen ? null : menu.key)}
                  className={[
                    'px-3 py-2 text-xs font-medium transition-colors border-b-2',
                    isOpen
                      ? 'text-blue-600 dark:text-blue-400 border-blue-600 dark:border-blue-400 bg-blue-50 dark:bg-blue-900/20'
                      : hasActive
                        ? 'text-gray-800 dark:text-gray-200 border-blue-600 dark:border-blue-400'
                        : 'text-gray-500 dark:text-gray-400 border-transparent hover:text-gray-700 dark:hover:text-gray-200 hover:bg-gray-50 dark:hover:bg-gray-800/50',
                  ].join(' ')}
                >
                  {menu.label}
                  <span className="ml-1 text-[10px] text-gray-400">{menu.items.length}</span>
                </button>
                {/* Dropdown */}
                {isOpen && (
                  <div className="absolute top-full left-0 mt-0 min-w-[220px] max-h-[60vh] overflow-y-auto bg-white dark:bg-[#1C2128] border border-gray-200 dark:border-gray-700 rounded-b-lg shadow-lg z-30">
                    {menu.items.map(item => {
                      const isActive = item.to === activePath
                      return (
                        <button
                          key={item.to}
                          onClick={() => {
                            setActivePath(item.to)
                            setOpenMenu(null)
                          }}
                          className={[
                            'flex items-center gap-2 w-full px-3 py-2 text-xs text-left transition-colors',
                            isActive
                              ? 'bg-blue-50 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300 font-medium'
                              : 'text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800',
                          ].join(' ')}
                        >
                          <item.Icon size={14} className="shrink-0 text-gray-400" />
                          <span className="truncate">{item.label}</span>
                        </button>
                      )
                    })}
                  </div>
                )}
              </div>
            )
          })}
        </div>
      </div>
      {/* Active page content */}
      <div className="flex-1 overflow-auto">
        {ActiveComponent ? <ActiveComponent /> : <div className="p-6 text-gray-400 text-sm">Select a dashboard from the menu above</div>}
      </div>
    </div>
  )
}

function Sidebar() {
  const location = useLocation()

  const pinnedItems = useMemo(
    () => NAV_ITEMS.filter(item => PINNED_PATHS.has(item.to)),
    []
  )

  // Check if current path belongs to a category (for highlighting when on individual routes)
  const activeCategoryKey = useMemo(() => {
    // If directly on a /cat/xxx route
    const catMatch = location.pathname.match(/^\/cat\/(\w+)/)
    if (catMatch) return catMatch[1]
    // If on an individual page route, find which category it belongs to
    const navItem = NAV_ITEMS.find(item => item.to === location.pathname)
    if (navItem && !PINNED_PATHS.has(navItem.to) && navItem.to !== '/settings') {
      return classifyNavItem(navItem.to, navItem.label)
    }
    return null
  }, [location.pathname])

  return (
    <aside className="flex flex-col w-56 min-h-screen text-gray-100 shrink-0" style={{ backgroundColor: 'var(--db-sidebar)' }}>
      {/* Brand */}
      <div className="flex items-center gap-2.5 px-5 py-4 border-b border-white/10">
        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64" className="w-7 h-7 shrink-0">
          <g transform="matrix(0.753,0,0,0.753,1.054,18.824)">
            <path fill="#db1905" d="M0,24.8V38.5L41.1,60 82.2,38.5V24.8L67.7,17.4 82.2,9.8V-3.8L82.1-3.8 41.1-25 .1-3.9H0V9.8L14.5,17.4 0,24.8"/>
            <polygon fill="#ff5224" transform="translate(0,-36)" points="0,60.8 41.1,82.3 82.2,60.8 67.7,53.4 41.1,67.3 14.5,53.4"/>
            <polygon fill="#ff5224" transform="translate(0,-36)" points="82.2,32.2 41.1,53.7 0,32.2 41.1,11"/>
          </g>
        </svg>
        <span className="text-sm font-bold leading-tight tracking-tight">
          AUS Energy<br />Copilot
        </span>
      </div>

      {/* Nav */}
      <nav className="flex-1 overflow-y-auto px-2 py-3">
        {/* Pinned items */}
        {pinnedItems.map(({ to, label, Icon }) => (
          <NavLink
            key={to}
            to={to}
            end={to === '/'}
            className={({ isActive }) =>
              `flex items-center gap-3 px-3 py-2 rounded-md text-sm font-medium transition-colors ${
                isActive ? 'bg-white/10 text-white' : 'text-gray-400 hover:bg-white/5 hover:text-white'
              }`
            }
          >
            <Icon size={18} />
            {label}
          </NavLink>
        ))}

        <div className="h-px bg-white/10 my-2 mx-2" />

        {/* Category links — flat, no accordion */}
        {GROUP_DEFS.map(group => {
          const isActive = activeCategoryKey === group.key
          return (
            <NavLink
              key={group.key}
              to={`/cat/${group.key}`}
              className={() =>
                `flex items-center gap-3 px-3 py-2 rounded-md text-sm font-medium transition-colors ${
                  isActive ? 'bg-white/10 text-white' : 'text-gray-400 hover:bg-white/5 hover:text-white'
                }`
              }
            >
              <group.Icon size={18} />
              {group.label}
            </NavLink>
          )
        })}
      </nav>

      {/* Footer */}
      <div className="border-t border-white/10 px-2 py-2">
        <NavLink
          to="/settings"
          className={({ isActive }) =>
            `flex items-center gap-3 px-3 py-2 rounded-md text-sm font-medium transition-colors ${
              isActive ? 'bg-white/10 text-white' : 'text-gray-400 hover:bg-white/5 hover:text-white'
            }`
          }
        >
          <SettingsIcon size={18} />
          Settings
        </NavLink>
        <div className="px-3 py-1 text-[10px] text-gray-500">
          Powered by Databricks
        </div>
      </div>
    </aside>
  )
}

function TopBar() {
  useDarkMode()
  return (
    <header className="h-12 flex items-center justify-between px-6 bg-white dark:bg-[#161B22] border-b border-gray-200 dark:border-[#30363D] shrink-0">
      <h1 className="text-base font-semibold text-gray-800 dark:text-gray-100">
        AUS Energy Copilot
      </h1>
      <div className="flex items-center gap-3 text-xs text-gray-500 dark:text-gray-400">
        <span className="flex items-center gap-1">
          <span className="w-2 h-2 rounded-full bg-green-500 inline-block"></span>
          NEM Live
        </span>
        <span>{new Date().toLocaleDateString('en-AU', { timeZone: 'Australia/Sydney', weekday: 'short', day: 'numeric', month: 'short' })} AEST</span>
      </div>
    </header>
  )
}

export default function App() {
  return (
    <BrowserRouter>
      <div className="flex h-screen bg-[#F5F7FA] dark:bg-[#0D1117] overflow-hidden">
        <Sidebar />
        <div className="flex flex-col flex-1 min-w-0 overflow-hidden">
          <TopBar />
          <main className="flex-1 overflow-auto">
            <Routes>
              <Route path="/cat/:groupKey" element={<CategoryPage />} />
              <Route path="/"          element={<Home />}       />
              <Route path="/live"      element={<LiveMarket />} />
              <Route path="/forecasts" element={<Forecasts />}  />
              <Route path="/copilot"   element={<Copilot />}    />
              <Route path="/genie"     element={<Genie />}      />
              <Route path="/alerts"        element={<Alerts />}       />
              <Route path="/monitoring"    element={<Monitoring />}   />
              <Route path="/market-depth"  element={<MarketDepth />}  />
              <Route path="/price-analysis"    element={<PriceAnalysis />}    />
              <Route path="/interconnectors"   element={<Interconnectors />}  />
              <Route path="/generator-fleet"  element={<GeneratorFleet />}   />
              <Route path="/market-notices"   element={<MarketNotices />}    />
              <Route path="/weather-demand"   element={<WeatherDemand />}    />
              <Route path="/bess"             element={<BessAnalytics />}    />
              <Route path="/trading-desk"    element={<TradingDesk />}      />
              <Route path="/sustainability" element={<Sustainability />}  />
              <Route path="/merit-order"   element={<MeritOrder />}       />
              <Route path="/ml-dashboard" element={<MlDashboardPage />}  />
              <Route path="/data-catalog" element={<DataCatalog />}       />
              <Route path="/scenario"       element={<ScenarioAnalysis />}  />
              <Route path="/load-duration" element={<LoadDuration />}      />
              <Route path="/trends"       element={<HistoricalTrends />}  />
              <Route path="/frequency"    element={<FrequencyAnalytics />} />
              <Route path="/futures"      element={<EnergyFutures />}      />
              <Route path="/registry"     element={<ParticipantRegistry />} />
              <Route path="/outages"      element={<OutageSchedule />}     />
              <Route path="/der"          element={<DerDashboard />}       />
              <Route path="/gas"          element={<GasMarket />}          />
              <Route path="/retail"       element={<RetailMarket />}       />
              <Route path="/network"      element={<NetworkAnalytics />}   />
              <Route path="/rez"          element={<RezInfrastructure />}  />
              <Route path="/curtailment"  element={<CurtailmentAnalytics />} />
              <Route path="/dsp"          element={<DemandResponse />}     />
              <Route path="/security"     element={<SystemSecurity />}     />
              <Route path="/bidding"      element={<BiddingAnalytics />}   />
              <Route path="/nem-events"   element={<NemEvents />}          />
              <Route path="/fcas-market"  element={<FcasMarket />}         />
              <Route path="/battery-econ" element={<BatteryEconomics />}  />
              <Route path="/battery-economics" element={<BatteryEconomics />}  />
              <Route path="/settlement"   element={<NemSettlement />}      />
              <Route path="/carbon"       element={<CarbonAnalytics />}   />
              <Route path="/hedging"      element={<HedgingAnalytics />}  />
              <Route path="/hydro"        element={<HydroStorage />}       />
              <Route path="/market-power" element={<MarketPower />}        />
              <Route path="/pasa"         element={<PasaAnalytics />}      />
              <Route path="/sra"          element={<SraAuction />}         />
              <Route path="/ppa"          element={<PpaMarket />}          />
              <Route path="/dispatch"     element={<DispatchAccuracy />}   />
              <Route path="/regulatory"   element={<RegulatoryTracker />}  />
              <Route path="/isp-tracker"  element={<IspTracker />}         />
              <Route path="/solar-ev"     element={<SolarEvAnalytics />}   />
              <Route path="/lrmc"         element={<LrmcAnalytics />}      />
              <Route path="/constraints"  element={<NetworkConstraints />}  />
              <Route path="/price-setter" element={<PriceSetterAnalytics />} />
              <Route path="/tariff"       element={<TariffAnalytics />}    />
              <Route path="/grid-mod"     element={<GridModernisation />}  />
              <Route path="/spot-cap"      element={<SpotCapAnalytics />} />
              <Route path="/causer-pays" element={<CauserPays />}       />
              <Route path="/wem"          element={<WemOverview />}         />
              <Route path="/inertia"      element={<InertiaAnalytics />}   />
              <Route path="/tnsp"         element={<TnspAnalytics />}      />
              <Route path="/surveillance" element={<MarketSurveillance />} />
              <Route path="/hydrogen"     element={<HydrogenAnalytics />} />
              <Route path="/offshore-wind" element={<OffshoreWind />}      />
              <Route path="/offshore-wind-pipeline" element={<OffshoreWindPipeline />} />
              <Route path="/cer"          element={<CerDashboard />}  />
              <Route path="/phes"         element={<PhesAnalytics />}      />
              <Route path="/safeguard"    element={<SafeguardAnalytics />} />
              <Route path="/transmission" element={<TransmissionProjects />} />
              <Route path="/dnsp"         element={<DnspAnalytics />}      />
              <Route path="/vpp"          element={<VppDashboard />}       />
              <Route path="/reform"       element={<MarketReformTracker />} />
              <Route path="/tuos"         element={<TuosAnalytics />}      />
              <Route path="/carbon-registry" element={<CarbonRegistry />}  />
              <Route path="/ev"           element={<EvCharging />}          />
              <Route path="/storage"      element={<StorageArbitrage />}    />
              <Route path="/demand-forecast" element={<DemandForecastAnalytics />} />
              <Route path="/rez-development" element={<RenewableEnergyZoneDevelopmentAnalytics />} />
              <Route path="/congestion"   element={<CongestionAnalytics />} />
              <Route path="/equity"           element={<EnergyEquity />}              />
              <Route path="/demand-response" element={<DemandResponseAnalytics />} />
              <Route path="/btm"            element={<BehindTheMeter />}         />
              <Route path="/rab"            element={<RabAnalytics />}           />
              <Route path="/realtime"       element={<NemRealTimeDashboard />}  />
              <Route path="/rit"            element={<RitAnalytics />}          />
              <Route path="/forward-curve"  element={<ForwardCurveAnalytics />} />
              <Route path="/coal-retirement" element={<CoalRetirement />}       />
              <Route path="/gas-gen"         element={<GasGenEconomics />}       />
              <Route path="/consumer-protection" element={<ConsumerProtection />} />
              <Route path="/efor"            element={<GeneratorAvailability />} />
              <Route path="/climate-risk"    element={<ClimateRiskAnalytics />} />
              <Route path="/smart-grid"      element={<SmartGridAnalytics />}   />
              <Route path="/minimum-demand"  element={<MinimumDemandAnalytics />} />
              <Route path="/market-events"  element={<MarketEventsAnalysis />}  />
              <Route path="/battery-tech"   element={<BatteryTechAnalytics />}  />
              <Route path="/community-energy" element={<CommunityEnergy />}     />
              <Route path="/asset-management" element={<AssetManagement />}    />
              <Route path="/decarbonization" element={<DecarbonizationPathway />} />
              <Route path="/nuclear-ldes"      element={<NuclearLongDuration />}    />
              <Route path="/nuclear-energy"   element={<NuclearEnergyAnalytics />} />
              <Route path="/bidding-behaviour" element={<BiddingBehaviour />}    />
              <Route path="/energy-poverty"    element={<EnergyPoverty />}       />
              <Route path="/spot-forecast"     element={<SpotForecastDashboard />} />
              <Route path="/hydrogen-economy"  element={<HydrogenEconomy />}      />
              <Route path="/hydrogen-economy-analytics" element={<HydrogenEconomyAnalytics />} />
              <Route path="/carbon-credit"     element={<CarbonCreditMarket />}  />
              <Route path="/grid-resilience"   element={<GridResilience />}      />
              <Route path="/ev-fleet"          element={<EvFleetCharging />}     />
              <Route path="/rec-market"        element={<RecMarket />}           />
              <Route path="/transmission-congestion" element={<TransmissionCongestion />} />
              <Route path="/derms-orchestration" element={<DermsOrchestration />}   />
              <Route path="/market-design"     element={<MarketDesignReform />}  />
              <Route path="/rez-capacity"      element={<RezCapacityTracking />} />
              <Route path="/retail-offer-comparison" element={<RetailOfferComparison />} />
              <Route path="/system-operator"   element={<SystemOperatorActions />} />
              <Route path="/network-tariff-reform" element={<NetworkTariffReform />} />
              <Route path="/spike-analysis"    element={<PriceSpikeAnalysis />}  />
              <Route path="/storage-revenue-stack" element={<StorageRevenueStack />} />
              <Route path="/solar-resource"    element={<SolarResourceAnalytics />} />
              <Route path="/futures-market-risk" element={<FuturesMarketRisk />}  />
              <Route path="/wind-resource"         element={<WindResourceAnalytics />}  />
              <Route path="/corporate-ppa-market" element={<CorporatePpaMarket />}    />
              <Route path="/microgrid-raps"    element={<MicrogridRaps />}         />
              <Route path="/market-liquidity"  element={<MarketLiquidity />}       />
              <Route path="/thermal-efficiency" element={<ThermalEfficiency />}  />
              <Route path="/industrial-demand-flex" element={<IndustrialDemandFlex />} />
              <Route path="/storage-lca"       element={<StorageLca />}          />
              <Route path="/interconnector-flow-analytics" element={<InterconnectorFlowAnalytics />} />
              <Route path="/isp-progress"                 element={<IspProgressTracker />}           />
              <Route path="/firming-technology-economics" element={<FirmingTechnologyEconomics />}  />
              <Route path="/demand-forecasting-models"   element={<DemandForecastingModels />}     />
              <Route path="/market-stress-testing"       element={<MarketStressTesting />}         />
              <Route path="/capacity-investment-signals" element={<CapacityInvestmentSignals />}   />
              <Route path="/frequency-control-analytics" element={<FrequencyControlAnalytics />}  />
              <Route path="/rec-certificate-tracking"    element={<RecCertificateTracking />}      />
              <Route path="/spot-market-depth"           element={<SpotMarketDepthAnalytics />}    />
              <Route path="/storage-tech-roadmap"        element={<StorageTechRoadmap />}           />
              <Route path="/renewable-integration-cost"  element={<RenewableIntegrationCost />}    />
              <Route path="/planned-outage-analytics"    element={<PlannedOutageAnalytics />}      />
              <Route path="/market-share-tracker"        element={<MarketShareTracker />}          />
              <Route path="/volatility-regime-analytics" element={<VolatilityRegimeAnalytics />}   />
              <Route path="/black-start-capability"      element={<BlackStartCapability />}         />
              <Route path="/ancillary-services-cost"    element={<AncillaryServicesCost />}        />
              <Route path="/cbam-trade-analytics"       element={<CbamTradeAnalytics />}          />
              <Route path="/congestion-revenue-analytics" element={<CongestionRevenueAnalytics />} />
              <Route path="/climate-physical-risk"       element={<ClimatePhysicalRisk />}         />
              <Route path="/energy-affordability"        element={<EnergyAffordabilityAnalytics />} />
              <Route path="/electrification-analytics"   element={<ElectrificationAnalytics />}     />
              <Route path="/electricity-export-infra"    element={<ElectricityExportInfra />}       />
              <Route path="/electricity-export-economics" element={<ElectricityExportEconomicsAnalytics />} />
              <Route path="/ldes-economics"              element={<LdesEconomicsAnalytics />}       />
              <Route path="/gas-transition-analytics"    element={<GasTransitionAnalytics />}       />
              <Route path="/prosumer-analytics"          element={<ProsumerAnalytics />}            />
              <Route path="/tnsp-analytics"                  element={<TnspAnalytics />}                        />
              <Route path="/dnsp-analytics"                element={<DnspPerformanceAnalytics />}             />
              <Route path="/reliability-standard-analytics" element={<ReliabilityStandardAnalytics />} />
              <Route path="/storage-optimisation-analytics" element={<StorageOptimisationAnalytics />} />
              <Route path="/settlement-analytics"          element={<SettlementAnalytics />}          />
              <Route path="/realtime-operations" element={<RealtimeOperationsDashboard />} />
              <Route path="/renewable-auction"           element={<RenewableAuctionAnalytics />} />
              <Route path="/voll-analytics"              element={<VollAnalytics />}                />
              <Route path="/demand-flexibility-analytics" element={<DemandFlexibilityAnalytics />} />
              <Route path="/futures-price-discovery"      element={<FuturesPriceDiscovery />}      />
              <Route path="/electricity-price-index"    element={<ElectricityPriceIndex />}      />
              <Route path="/interconnector-upgrade-analytics" element={<InterconnectorUpgradeAnalytics />} />
              <Route path="/mlf-analytics"               element={<MlfAnalytics />}               />
              <Route path="/csp-analytics"               element={<CspAnalytics />}               />
              <Route path="/carbon-intensity-analytics"  element={<CarbonIntensityAnalytics />}   />
              <Route path="/network-tariff-reform-analytics" element={<NetworkTariffReformAnalytics />} />
              <Route path="/tariff-cross-subsidy"            element={<TariffCrossSubsidyAnalytics />} />
              <Route path="/ai-digital-twin-analytics"   element={<AiDigitalTwinAnalytics />}     />
              <Route path="/esoo-adequacy-analytics"     element={<EsooAdequacyAnalytics />}      />
              <Route path="/social-licence-analytics"    element={<SocialLicenceAnalytics />}     />
              <Route path="/electricity-options"          element={<ElectricityOptionsAnalytics />} />
              <Route path="/grid-forming-inverter"       element={<GridFormingInverterAnalytics />} />
              <Route path="/capacity-mechanism"          element={<CapacityMechanismAnalytics />}  />
              <Route path="/demand-forecast-accuracy" element={<DemandForecastAccuracyAnalytics />} />
              <Route path="/transmission-investment" element={<TransmissionInvestmentAnalytics />} />
              <Route path="/rez-progress"      element={<RezProgressAnalytics />} />
              <Route path="/storage-revenue"   element={<StorageRevenueAnalytics />} />
              <Route path="/carbon-price-pathway" element={<CarbonPricePathwayAnalytics />} />
              <Route path="/spot-price-forecast" element={<SpotPriceForecastAnalytics />} />
              <Route path="/ancillary-cost-allocation" element={<AncillaryCostAllocationAnalytics />} />
              <Route path="/wholesale-liquidity" element={<MarketLiquidityAnalytics />} />
              <Route path="/generator-retirement" element={<GeneratorRetirementAnalytics />} />
              <Route path="/consumer-hardship"   element={<ConsumerHardshipAnalytics />}   />
              <Route path="/dsr-aggregator"      element={<DsrAggregatorAnalytics />}      />
              <Route path="/power-system-events" element={<PowerSystemEventsAnalytics />}  />
              <Route path="/merchant-renewable"  element={<MerchantRenewableAnalytics />}  />
              <Route path="/retailer-competition" element={<RetailerCompetitionAnalytics />} />
              <Route path="/storage-cost-curves" element={<StorageCostCurvesAnalytics />} />
              <Route path="/extreme-weather-resilience" element={<ExtremeWeatherResilienceAnalytics />} />
              <Route path="/spot-price-volatility-regime" element={<SpotPriceVolatilityRegimeAnalytics />} />
              <Route path="/industrial-electrification" element={<IndustrialElectrificationAnalytics />} />
              <Route path="/offshore-wind-dev-analytics" element={<OffshoreWindDevAnalytics />} />
              <Route path="/pumped-hydro-resource-assessment" element={<PumpedHydroResourceAssessmentAnalytics />} />
              <Route path="/frequency-control-performance" element={<FrequencyControlPerformanceAnalytics />} />
              <Route path="/cost-reflective-tariff-reform" element={<CostReflectiveTariffReformAnalytics />} />
              <Route path="/ev-fleet-grid-impact"          element={<EVFleetGridImpactAnalytics />}          />
              <Route path="/nem-market-microstructure"     element={<NEMMarketMicrostructureAnalytics />}     />
              <Route path="/rooftop-solar-grid"            element={<RooftopSolarGridAnalytics />}            />
              <Route path="/rec-market-analytics"          element={<RECMarketAnalytics />}                   />
              <Route path="/energy-poverty-analytics"    element={<EnergyPovertyAnalytics />}              />
              <Route path="/hedge-effectiveness"         element={<HedgeEffectivenessAnalytics />}         />
              <Route path="/cbam-trade-exposure"         element={<CBAMTradeExposureAnalytics />}          />
              <Route path="/demand-response-programs"   element={<DemandResponseProgramAnalytics />}     />
              <Route path="/interconnector-congestion"   element={<InterconnectorCongestionAnalytics />}  />
              <Route path="/ppa-market"                  element={<PPAMarketAnalytics />}                 />
              <Route path="/ppa-structuring"             element={<PPAStructuringAnalytics />}            />
              <Route path="/battery-dispatch-strategy"   element={<BatteryDispatchStrategyAnalytics />}   />
              <Route path="/generation-mix-transition"   element={<GenerationMixTransitionAnalytics />}   />
              <Route path="/storage-duration-economics" element={<StorageDurationEconomicsAnalytics />}  />
              <Route path="/ancillary-market-depth"    element={<AncillaryServicesMarketDepthAnalytics />} />
              <Route path="/sra-analytics"             element={<SRAAnalyticsPage />}                      />
              <Route path="/spot-market-stress"        element={<SpotMarketStressAnalytics />}             />
              <Route path="/electricity-workforce"     element={<ElectricityWorkforceAnalytics />}         />
              <Route path="/rez-transmission"          element={<REZTransmissionAnalytics />}               />
              <Route path="/network-regulatory-framework" element={<NetworkRegulatoryFrameworkAnalytics />} />
              <Route path="/price-model-comparison"       element={<PriceModelComparisonAnalytics />}       />
              <Route path="/gas-electricity-nexus"         element={<GasElectricityNexusAnalytics />}        />
              <Route path="/bidding-compliance"           element={<BiddingComplianceAnalytics />}          />
              <Route path="/community-energy-analytics"  element={<CommunityEnergyAnalytics />}            />
              <Route path="/grid-cybersecurity"           element={<GridCybersecurityAnalytics />}           />
              <Route path="/market-participant-financial" element={<MarketParticipantFinancialAnalytics />}  />
              <Route path="/digital-transformation"       element={<DigitalTransformationAnalytics />}       />
              <Route path="/energy-transition-finance"   element={<EnergyTransitionFinanceAnalytics />}     />
              <Route path="/negative-price-events"        element={<NegativePriceEventAnalytics />}          />
              <Route path="/cer-orchestration"             element={<CEROrchestrationAnalytics />}            />
              <Route path="/system-load-balancing"         element={<SystemLoadBalancingAnalytics />}          />
              <Route path="/carbon-accounting"             element={<CarbonAccountingAnalytics />}             />
              <Route path="/wholesale-bidding-strategy"    element={<WholesaleBiddingStrategyAnalytics />}     />
              <Route path="/ldes-analytics"                element={<LDESAnalytics />}                         />
              <Route path="/emergency-management"          element={<EmergencyManagementAnalytics />}           />
              <Route path="/consumer-switching-retail-churn" element={<ConsumerSwitchingRetailChurnAnalytics />} />
              <Route path="/solar-thermal-csp" element={<SolarThermalCSPAnalytics />} />
              <Route path="/nem-post-reform-market-design" element={<NEMPostReformMarketDesignAnalytics />} />
              <Route path="/electricity-price-forecasting-models" element={<ElectricityPriceForecastingModelAnalytics />} />
              <Route path="/large-industrial-demand" element={<LargeIndustrialDemandAnalytics />} />
              <Route path="/network-investment-pipeline" element={<NetworkInvestmentPipelineAnalytics />} />
              <Route path="/nem-demand-forecast" element={<NEMDemandForecastAnalytics />} />
              <Route path="/hydrogen-fuel-cell-vehicles" element={<HydrogenFuelCellVehicleAnalytics />} />
              <Route path="/spot-price-spike-prediction" element={<SpotPriceSpikePredictionAnalytics />} />
              <Route path="/grid-edge-technology" element={<GridEdgeTechnologyAnalytics />} />
              <Route path="/bess-degradation" element={<EnergyStorageDegradationAnalytics />} />
              <Route path="/clean-hydrogen-production-cost" element={<CleanHydrogenProductionCostAnalytics />} />
              <Route path="/ancillary-services-procurement" element={<AncillaryServicesProcurementAnalytics />} />
              <Route path="/rez-connection-queue" element={<REZConnectionQueueAnalytics />} />
              <Route path="/australian-carbon-policy" element={<AustralianCarbonPolicyAnalytics />} />
              <Route path="/market-design-simulation" element={<MarketDesignSimulationAnalytics />} />
              <Route path="/power-system-stability" element={<PowerSystemStabilityAnalytics />} />
              <Route path="/energy-retail-competition" element={<EnergyRetailCompetitionAnalytics />} />
              <Route path="/clean-energy-finance" element={<CleanEnergyFinanceAnalytics />} />
              <Route path="/nuclear-energy-economics" element={<NuclearEnergyEconomicsAnalytics />} />
              <Route path="/behind-meter-commercial" element={<BehindMeterCommercialAnalytics />} />
              <Route path="/capacity-investment-scheme" element={<CapacityInvestmentSchemeAnalytics />} />
              <Route path="/demand-flexibility-market" element={<DemandFlexibilityMarketAnalytics />} />
              <Route path="/energy-asset-life-extension" element={<EnergyAssetLifeExtensionAnalytics />} />
              <Route path="/green-ammonia-export" element={<GreenAmmoniaExportAnalytics />} />
              <Route path="/electricity-export-cable" element={<ElectricityExportCableAnalytics />} />
              <Route path="/industrial-decarbonisation" element={<IndustrialDecarbonisationAnalytics />} />
              <Route path="/community-energy-storage" element={<CommunityEnergyStorageAnalytics />} />
              <Route path="/nem-generation-mix" element={<NEMGenerationMixAnalytics />} />
              <Route path="/consumer-energy-affordability" element={<ConsumerEnergyAffordabilityAnalytics />} />
              <Route path="/electricity-price-risk" element={<ElectricityPriceRiskAnalytics />} />
              <Route path="/ev-fleet-depot"    element={<EVFleetDepotAnalytics />}          />
              <Route path="/wind-farm-wake"    element={<WindFarmWakeAnalytics />}          />
              <Route path="/market-bidding-strategy" element={<MarketBiddingStrategyAnalytics />} />
              <Route path="/solar-pv-soiling"  element={<SolarPVSoilingAnalytics />} />
              <Route path="/ot-ics-cyber-security" element={<OTICSCyberSecurityAnalytics />} />
              <Route path="/stpasa-adequacy" element={<STPASAAdequacyAnalytics />} />
              <Route path="/generator-performance-standards" element={<GeneratorPerformanceStandardsAnalytics />} />
              <Route path="/biomass-bioenergy" element={<BiomassBioenergyAnalytics />} />
              <Route path="/electricity-frequency-performance" element={<ElectricityFrequencyPerformanceAnalytics />} />
              <Route path="/lgc-market"                      element={<LGCMarketAnalytics />}                       />
              <Route path="/wave-tidal-ocean" element={<WaveTidalOceanAnalytics />} />
              <Route path="/reactive-power-voltage" element={<ReactivePowerVoltageAnalytics />} />
              <Route path="/battery-revenue-stack" element={<BatteryRevenueStackAnalytics />} />
              <Route path="/digital-energy-twin" element={<DigitalEnergyTwinAnalytics />} />
              <Route path="/network-protection-system" element={<NetworkProtectionSystemAnalytics />} />
              <Route path="/pumped-hydro-dispatch" element={<PumpedHydroDispatchAnalytics />} />
              <Route path="/retail-market-design" element={<RetailMarketDesignAnalytics />} />
              <Route path="/spot-market-depth-x" element={<SpotMarketDepthXAnalytics />} />
              <Route path="/solar-farm-operations" element={<SolarFarmOperationsAnalytics />} />
              <Route path="/distribution-network-planning" element={<DistributionNetworkPlanningAnalytics />} />
              <Route path="/grid-flexibility-services" element={<GridFlexibilityServicesAnalytics />} />
              <Route path="/hydrogen-refuelling-station" element={<HydrogenRefuellingStationAnalytics />} />
              <Route path="/offshore-wind-finance"       element={<OffshoreWindFinanceAnalytics />}       />
              <Route path="/carbon-offset-project"     element={<CarbonOffsetProjectAnalytics />}      />
              <Route path="/power-grid-climate-resilience" element={<PowerGridClimateResilienceAnalytics />} />
              <Route path="/energy-storage-tech-comparison" element={<EnergyStorageTechComparisonAnalytics />} />
              <Route path="/power-to-x-economics" element={<PowerToXEconomicsAnalytics />} />
              <Route path="/electricity-market-microstructure" element={<ElectricityMarketMicrostructureAnalytics />} />
              <Route path="/grid-decarbonisation-pathway" element={<GridDecarbonisationPathwayAnalytics />} />
              <Route path="/rooftop-solar-network-impact" element={<RooftopSolarNetworkImpactAnalytics />} />
              <Route path="/electricity-network-tariff-reform" element={<ElectricityNetworkTariffReformAnalytics />} />
              <Route path="/long-duration-energy-storage" element={<LongDurationEnergyStorageAnalytics />} />
              <Route path="/hydrogen-pipeline-infrastructure" element={<HydrogenPipelineInfrastructureAnalytics />} />
              <Route path="/carbon-capture-storage-project"  element={<CarbonCaptureStorageProjectAnalytics />}  />
              <Route path="/energy-poverty-vulnerable-consumer" element={<EnergyPovertyVulnerableConsumerAnalytics />} />
              <Route path="/nuclear-small-modular-reactor" element={<NuclearSmallModularReactorAnalytics />} />
              <Route path="/electricity-market-transparency" element={<ElectricityMarketTransparencyAnalytics />} />
              <Route path="/geothermal-energy-development" element={<GeothermalEnergyDevelopmentAnalytics />} />
              <Route path="/solar-thermal-power-plant" element={<SolarThermalPowerPlantAnalytics />} />
              <Route path="/energy-trading-algorithmic-strategy" element={<EnergyTradingAlgorithmicStrategyAnalytics />} />
              <Route path="/ev-grid-integration-v2g" element={<EVGridIntegrationV2GAnalytics />} />
              <Route path="/biomethane-gas-grid-injection" element={<BiomethaneGasGridInjectionAnalytics />} />
              <Route path="/electricity-market-forecasting-accuracy" element={<ElectricityMarketForecastingAccuracyAnalytics />} />
              <Route path="/national-energy-transition-investment" element={<NationalEnergyTransitionInvestmentAnalytics />} />
              <Route path="/electricity-spot-price-seasonality" element={<ElectricitySpotPriceSeasonalityAnalytics />} />
              <Route path="/grid-congestion-constraint" element={<GridCongestionConstraintAnalytics />} />
              <Route path="/electricity-market-competition-concentration" element={<ElectricityMarketCompetitionConcentrationAnalytics />} />
              <Route path="/renewable-energy-zone-development" element={<RenewableEnergyZoneDevelopmentAnalytics />} />
              <Route path="/battery-storage-degradation-lifetime" element={<BatteryStorageDegradationLifetimeAnalytics />} />
              <Route path="/electricity-consumer-switching-churn" element={<ElectricityConsumerSwitchingChurnAnalytics />} />
              <Route path="/nem-inertia-synchronous-condenser" element={<NEMInertiaSynchronousCondenserAnalytics />} />
              <Route path="/offshore-wind-leasing-site" element={<OffshoreWindLeasingSiteAnalytics />} />
              <Route path="/electricity-market-regulatory-appeals" element={<ElectricityMarketRegulatoryAppealsAnalytics />} />
              <Route path="/distributed-energy-resource-management" element={<DistributedEnergyResourceManagementAnalytics />} />
              <Route path="/market-price-formation-review" element={<MarketPriceFormationReviewAnalytics />} />
              <Route path="/residential-solar-self-consumption" element={<ResidentialSolarSelfConsumptionAnalytics />} />
              <Route path="/energy-infrastructure-cyber-threat" element={<EnergyInfrastructureCyberThreatAnalytics />} />
              <Route path="/wholesale-gas-market" element={<WholesaleGasMarketAnalytics />} />
              <Route path="/electricity-demand-forecasting-ml" element={<ElectricityDemandForecastingMLAnalytics />} />
              <Route path="/energy-storage-merchant-revenue" element={<EnergyStorageMerchantRevenueAnalytics />} />
              <Route path="/industrial-electrification-x" element={<IndustrialElectrificationXAnalytics />} />
              <Route path="/transmission-access-reform" element={<TransmissionAccessReformAnalytics />} />
              <Route path="/hydrogen-export-terminal" element={<HydrogenExportTerminalAnalytics />} />
              <Route path="/grid-edge-technology-x" element={<GridEdgeTechnologyXAnalytics />} />
              <Route path="/energy-retailer-margin" element={<EnergyRetailerMarginAnalytics />} />
              <Route path="/ev-fleet-charging" element={<EvFleetChargingAnalytics />} />
              <Route path="/carbon-border-adjustment" element={<CarbonBorderAdjustmentAnalytics />} />
              <Route path="/power-purchase-agreement-market" element={<PowerPurchaseAgreementMarketAnalytics />} />
              <Route path="/distributed-solar-forecasting" element={<DistributedSolarForecastingAnalytics />} />
              <Route path="/electricity-market-liquidity" element={<ElectricityMarketLiquidityAnalytics />} />
              <Route path="/energy-transition-finance-x" element={<EnergyTransitionFinanceXAnalytics />} />
              <Route path="/nem-frequency-control" element={<NemFrequencyControlAnalytics />} />
              <Route path="/battery-second-life" element={<BatterySecondLifeAnalytics />} />
              <Route path="/utility-solar-farm-operations" element={<UtilitySolarFarmOperationsAnalytics />} />
              <Route path="/wind-farm-wake-effect" element={<WindFarmWakeEffectAnalytics />} />
              <Route path="/energy-poverty-hardship" element={<EnergyPovertyHardshipAnalytics />} />
              <Route path="/electricity-network-capital-investment" element={<ElectricityNetworkCapitalInvestmentAnalytics />} />
              <Route path="/gas-to-power-transition" element={<GasToPowerTransitionAnalytics />} />
              <Route path="/carbon-offset-market" element={<CarbonOffsetMarketAnalytics />} />
              <Route path="/power-system-stability-x" element={<PowerSystemStabilityXAnalytics />} />
              <Route path="/aemo-market-operations" element={<AemoMarketOperationsAnalytics />} />
              <Route path="/renewable-energy-certificate" element={<RenewableEnergyCertificateAnalytics />} />
              <Route path="/energy-storage-dispatch-optimisation" element={<EnergyStorageDispatchOptimisationAnalytics />} />
              <Route path="/offshore-wind-project-finance" element={<OffshoreWindProjectFinanceAnalytics />} />
              <Route path="/national-energy-market-reform" element={<NationalEnergyMarketReformAnalytics />} />
              <Route path="/electricity-market-price-formation" element={<ElectricityMarketPriceFormationAnalytics />} />
              <Route path="/rez-auction-cis" element={<RezAuctionCisAnalytics />} />
              <Route path="/grid-modernisation-digital-twin" element={<GridModernisationDigitalTwinAnalytics />} />
              <Route path="/energy-market-credit-risk" element={<EnergyMarketCreditRiskAnalytics />} />
              <Route path="/electricity-consumer-behaviour" element={<ElectricityConsumerBehaviourAnalytics />} />
              <Route path="/thermal-coal-power-transition" element={<ThermalCoalPowerTransitionAnalytics />} />
              <Route path="/demand-response-aggregator" element={<DemandResponseAggregatorAnalytics />} />
              <Route path="/energy-commodity-trading" element={<EnergyCommodityTradingAnalytics />} />
              <Route path="/network-tariff-design-reform" element={<NetworkTariffDesignReformAnalytics />} />
              <Route path="/hydrogen-valley-cluster" element={<HydrogenValleyClusterAnalytics />} />
              <Route path="/nem-congestion-rent" element={<NemCongestionRentAnalytics />} />
              <Route path="/electricity-retailer-churn" element={<ElectricityRetailerChurnAnalytics />} />
              <Route path="/energy-asset-maintenance" element={<EnergyAssetMaintenanceAnalytics />} />
              <Route path="/coal-seam-gas" element={<CoalSeamGasAnalytics />} />
              <Route path="/ev-battery-technology" element={<EvBatteryTechnologyAnalytics />} />
              <Route path="/nem-demand-forecasting-accuracy" element={<NemDemandForecastingAccuracyAnalytics />} />
              <Route path="/power-system-inertia" element={<PowerSystemInertiaAnalytics />} />
              <Route path="/electricity-network-investment-deferral" element={<ElectricityNetworkInvestmentDeferralAnalytics />} />
              <Route path="/rez-capacity-factor" element={<RezCapacityFactorAnalytics />} />
              <Route path="/energy-retailer-hedging" element={<EnergyRetailerHedgingAnalytics />} />
              <Route path="/gas-power-plant-flexibility" element={<GasPowerPlantFlexibilityAnalytics />} />
              <Route path="/solar-irradiance-resource" element={<SolarIrradianceResourceAnalytics />} />
              <Route path="/electricity-price-cap-intervention" element={<ElectricityPriceCapInterventionAnalytics />} />
              <Route path="/biogas-landfill" element={<BiogasLandfillAnalytics />} />
              <Route path="/wind-resource-variability" element={<WindResourceVariabilityAnalytics />} />
              <Route path="/energy-storage-duration" element={<EnergyStorageDurationAnalytics />} />
              <Route path="/nem-settlement-residue-auction" element={<NemSettlementResidueAuctionAnalytics />} />
              <Route path="/hydrogen-electrolysis-cost" element={<HydrogenElectrolysisCostAnalytics />} />
              <Route path="/electricity-demand-elasticity" element={<ElectricityDemandElasticityAnalytics />} />
              <Route path="/nuclear-energy-feasibility" element={<NuclearEnergyFeasibilityAnalytics />} />
              <Route path="/transmission-congestion-revenue" element={<TransmissionCongestionRevenueAnalytics />} />
              <Route path="/electricity-market-design-reform" element={<ElectricityMarketDesignReformAnalytics />} />
              <Route path="/carbon-capture-utilisation" element={<CarbonCaptureUtilisationAnalytics />} />
              <Route path="/grid-scale-battery-degradation" element={<GridScaleBatteryDegradationAnalytics />} />
              <Route path="/electricity-export" element={<AustraliaElectricityExportAnalytics />} />
              <Route path="/dsm-programs"      element={<DemandSideManagementProgramAnalytics />} />
              <Route path="/grid-topology"     element={<PowerGridTopologyAnalytics />} />
              <Route path="/rooftop-solar-fit" element={<RooftopSolarFeedInTariffAnalytics />} />
              <Route path="/lng-export"        element={<LngExportAnalytics />}  />
              <Route path="/settings"          element={<Settings />}            />
              <Route path="/community-microgrids" element={<EnergyCommunityMicrogridAnalytics />} />
              <Route path="/ewml-dashboard" element={<ElectricityWholesaleMarketLiquidityAnalytics />} />
              <Route path="/marine-energy"  element={<TidalWaveMarineEnergyAnalytics />}               />
              <Route path="/geothermal-energy" element={<GeothermalEnergyAnalytics />} />
              <Route path="/storage-arbitrage" element={<EnergyStorageArbitrageAnalytics />} />
              <Route path="/carbon-border-adjustment-x" element={<CarbonBorderAdjustmentXAnalytics />} />
              <Route path="/fcas-procurement" element={<FcasProcurementAnalytics />} />
              <Route path="/electricity-futures-options" element={<ElectricityFuturesOptionsAnalytics />} />
              <Route path="/rec-lgc-stc" element={<RenewableEnergyCertificateXAnalytics />} />
              <Route path="/der-management-x" element={<DistributedEnergyResourceManagementXAnalytics />} />
              <Route path="/coal-mine-energy" element={<CoalMineEnergyAnalytics />} />
              <Route path="/nem-5min-settlement" element={<NemFiveMinuteSettlementAnalytics />} />
              <Route path="/network-congestion-relief" element={<NetworkCongestionReliefAnalytics />} />
              <Route path="/market-concentration-bidding" element={<MarketConcentrationBiddingAnalytics />} />
              <Route path="/industrial-energy-efficiency" element={<IndustrialEnergyEfficiencyAnalytics />} />
              <Route path="/retailer-financial-health" element={<RetailerFinancialHealthAnalytics />} />
              <Route path="/solar-farm-performance" element={<SolarFarmPerformanceAnalytics />} />
              <Route path="/gas-network-pipeline" element={<GasNetworkPipelineAnalytics />} />
              <Route path="/electricity-price-forecasting" element={<ElectricityPriceForecastingAnalytics />} />
              <Route path="/network-asset-life-cycle" element={<NetworkAssetLifeCycleAnalytics />} />
              <Route path="/wind-farm-wake-turbine" element={<WindFarmWakeTurbineAnalytics />} />
              <Route path="/energy-poverty-hardship-x" element={<EnergyPovertyHardshipXAnalytics />} />
              <Route path="/hydrogen-refuelling-transport" element={<HydrogenRefuellingTransportAnalytics />} />
              <Route path="/electricity-spot-price-events" element={<ElectricitySpotPriceEventAnalytics />} />
              <Route path="/large-scale-renewable-auction" element={<LargeScaleRenewableAuctionAnalytics />} />
              <Route path="/nem-ancillary-services" element={<NemAncillaryServicesAnalytics />} />
              <Route path="/australian-carbon-credit" element={<AustralianCarbonCreditAnalytics />} />
              <Route path="/ev-grid-integration" element={<ElectricVehicleGridIntegrationAnalytics />} />
              <Route path="/generator-capacity-adequacy" element={<GeneratorCapacityAdequacyAnalytics />} />
              <Route path="/smart-grid-cybersecurity" element={<SmartGridCybersecurityAnalytics />} />
              <Route path="/pumped-hydro-reservoir" element={<PumpedHydroReservoirAnalytics />} />
              <Route path="/etjj-dashboard" element={<EnergyTransitionJobsAnalytics />} />
              <Route path="/nifr-dashboard" element={<InterconnectorFlowRightsAnalytics />} />
              <Route path="/cefa-dashboard-x" element={<CleanEnergyFinanceXAnalytics />} />
              <Route path="/bess-performance" element={<BessPerformanceAnalytics />} />
              <Route path="/elca-dashboard" element={<LoadCurveAnalytics />} />
              <Route path="/ngts-dashboard" element={<NaturalGasTradingAnalytics />} />
              <Route path="/aeos-dashboard" element={<EnergyOptimisationAnalytics />} />
              <Route path="/aemc-rule-change" element={<AemcRuleChangeAnalytics />} />
              <Route path="/renx-dashboard" element={<RenewableExportAnalytics />} />
              <Route path="/cppa-dashboard-x" element={<CorporatePpaAnalytics />} />
              <Route path="/nzem-dashboard" element={<NetZeroEmissionsAnalytics />} />
              <Route path="/epsa-dashboard" element={<PriceSensitivityAnalytics />} />
              <Route path="/grpt-dashboard" element={<GridReliabilityAnalytics />} />
              <Route path="/mats-dashboard" element={<MarketTradingStrategyAnalytics />} />
              <Route path="/emga-dashboard" element={<EmergingMarketsAnalytics />} />
              <Route path="/epro-dashboard" element={<PortfolioRiskOptimisationAnalytics />} />
              <Route path="/aehm-dashboard" element={<EnergyHubMicrostructureAnalytics />} />
              <Route path="/fmrp-dashboard" element={<FrequencyReservePlanningAnalytics />} />
              <Route path="/daro-dashboard" element={<DistributedAssetOptimisationAnalytics />} />
              <Route path="/ecsa-dashboard" element={<ConsumerSegmentationAnalytics />} />
              <Route path="/gena-dashboard" element={<GenerationExpansionAnalytics />} />
              <Route path="/nema-dashboard" element={<MarketAnomalyDetectionAnalytics />} />
              <Route path="/wcms-dashboard" element={<WindCapacityMarketAnalytics />} />
              <Route path="/spar-dashboard" element={<SolarParkRegistryAnalytics />} />
              <Route path="/energy-storage-duration-x" element={<EnergyStorageDurationXAnalytics />} />
              <Route path="/offshore-wind-development" element={<OffshoreWindDevelopmentAnalytics />} />
              <Route path="/nem-participant-financial" element={<NemParticipantFinancialAnalytics />} />
              <Route path="/green-tariff-hydrogen" element={<GreenTariffHydrogenAnalytics />} />
              <Route path="/nem-price-review" element={<NemPriceReviewAnalytics />} />
              <Route path="/renewable-certificate-nem" element={<RenewableCertificateNemAnalytics />} />
              <Route path="/demand-curve-price-anchor" element={<DemandCurvePriceAnchorAnalytics />} />
              <Route path="/market-evolution-policy" element={<MarketEvolutionPolicyAnalytics />} />
              <Route path="/energy-grid-topology" element={<EnergyGridTopologyAnalytics />} />
              <Route path="/carbon-voluntary-exchange" element={<CarbonVoluntaryExchangeAnalytics />} />
              <Route path="/renewable-market-sensitivity" element={<RenewableMarketSensitivityAnalytics />} />
              <Route path="/natural-gas-pipeline" element={<NaturalGasPipelineAnalytics />} />
              <Route path="/battery-chemistry-risk" element={<BatteryChemistryRiskAnalytics />} />
              <Route path="/aemo-5min-settlement" element={<Aemo5MinSettlementAnalytics />} />
              <Route path="/retail-competition" element={<ElectricityRetailCompetitionAnalytics />} />
              <Route path="/demand-response-aggregation" element={<DemandResponseAggregationAnalytics />} />
              <Route path="/grid-frequency" element={<GridFrequencyResponseAnalytics />} />
              <Route path="/hydrogen-economy-outlook" element={<HydrogenEconomyOutlookAnalytics />} />
              <Route path="/network-augmentation" element={<NetworkAugmentationDeferralAnalytics />} />
              <Route path="/ev-fleet-grid" element={<EvFleetGridIntegrationAnalytics />} />
              <Route path="/grid-emissions-intensity" element={<GridEmissionsIntensityAnalytics />} />
              <Route path="/microgrid-resilience" element={<MicrogridResilienceAnalytics />} />
              <Route path="/power-quality" element={<PowerQualityMonitoringAnalytics />} />
              <Route path="/vpp-operations" element={<VirtualPowerPlantOperationsAnalytics />} />
              <Route path="/pumped-hydro" element={<PumpedHydroStorageAnalytics />} />
              <Route path="/smart-meter" element={<SmartMeterDataAnalytics />} />
              <Route path="/east-coast-gas" element={<EastCoastGasMarketAnalytics />} />
              <Route path="/isp-analytics" element={<IntegratedSystemPlanAnalytics />} />
              <Route path="/community-battery" element={<CommunityBatteryAnalytics />} />
              <Route path="/energy-cyber-security" element={<EnergySectorCyberSecurityAnalytics />} />
              <Route path="/wholesale-market-reform" element={<WholesaleMarketReformAnalytics />} />
            </Routes>
          </main>
        </div>
      </div>
    </BrowserRouter>
  )
}
