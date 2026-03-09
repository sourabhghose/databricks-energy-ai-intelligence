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
  Plus,
  Upload,
  Rocket,
  CloudRain,
  ChevronDown,
} from 'lucide-react'

import NemInfrastructureMap from './pages/NemInfrastructureMap'
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
import DealCapture from './pages/DealCapture'
import PortfolioPage from './pages/Portfolio'
import TradeBlotter from './pages/TradeBlotter'
import ForwardCurves from './pages/ForwardCurves'
import RiskDashboard from './pages/RiskDashboard'
import MarketReplay from './pages/MarketReplay'
import MarketBriefs from './pages/MarketBriefs'
import BiddingDashboard from './pages/BiddingDashboard'
import BatteryDashboard from './pages/BatteryDashboard'
import GasMarketDashboard from './pages/GasMarketDashboard'
import WemDashboard from './pages/WemDashboard'
import ComplianceDashboard from './pages/ComplianceDashboard'
import EnvironmentalsDashboard from './pages/EnvironmentalsDashboard'
import ReportsLibrary from './pages/ReportsLibrary'

const NAV_ITEMS = [
  { to: '/',             label: 'Home',         Icon: LayoutDashboard },
  { to: '/live',         label: 'Live Market',  Icon: Zap             },
  { to: '/realtime-operations', label: 'Live Ops Dashboard', Icon: Radio },
  { to: '/market-replay',  label: 'Market Replay',  Icon: Clock           },
  { to: '/market-briefs',  label: 'Market Briefs',  Icon: FileText        },
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
  { to: '/deal-capture',    label: 'Deal Capture',     Icon: Plus           },
  { to: '/portfolio',       label: 'Portfolio',        Icon: Briefcase      },
  { to: '/trade-blotter',   label: 'Trade Blotter',    Icon: Receipt        },
  { to: '/forward-curves',  label: 'Forward Curves',   Icon: TrendingUp     },
  { to: '/risk-dashboard',  label: 'Risk Dashboard',   Icon: Shield         },
  { to: '/bidding-dashboard', label: 'Bidding Dashboard', Icon: BarChart2    },
  { to: '/battery-dispatch',  label: 'Battery Dispatch',  Icon: Battery      },
  { to: '/gas-market',        label: 'Gas Market Dashboard', Icon: Flame     },
  { to: '/wem-dashboard',     label: 'WEM Dashboard',     Icon: Building2    },
  { to: '/compliance',        label: 'Compliance Dashboard', Icon: FileText  },
  { to: '/environmentals',    label: 'Environmentals',    Icon: Leaf         },
  { to: '/reports',           label: 'Reports Library',   Icon: FileText     },
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
  '/deal-capture': DealCapture,
  '/portfolio': PortfolioPage,
  '/trade-blotter': TradeBlotter,
  '/forward-curves': ForwardCurves,
  '/risk-dashboard': RiskDashboard,
  '/market-replay': MarketReplay,
  '/market-briefs': MarketBriefs,
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
  '/nem-map': NemInfrastructureMap,
}

// ---------------------------------------------------------------------------
// Sidebar — Collapsible Front / Middle / Back Office sections
// ---------------------------------------------------------------------------

const PINNED_PATHS = new Set(['/', '/live', '/copilot', '/genie', '/alerts'])

const DASHBOARD_LINKS: { to: string; label: string; Icon: React.ComponentType<{ size?: number; className?: string }> }[] = [
  // Trading & Risk
  { to: '/deal-capture',      label: 'Deal Capture',   Icon: Plus },
  { to: '/portfolio',         label: 'Portfolio',      Icon: Briefcase },
  { to: '/forward-curves',    label: 'Forward Curves', Icon: TrendingUp },
  { to: '/risk-dashboard',    label: 'Risk',           Icon: Shield },
  // Market Dashboards
  { to: '/bidding-dashboard', label: 'Bidding',        Icon: BarChart2 },
  { to: '/battery-dispatch',  label: 'Battery',        Icon: Battery },
  { to: '/gas-market',        label: 'Gas Market',     Icon: Flame },
  { to: '/wem-dashboard',     label: 'WEM',            Icon: Building2 },
  // Operations & Compliance
  { to: '/nem-map',           label: 'NEM Map',        Icon: MapIcon },
  { to: '/market-briefs',     label: 'Market Briefs',  Icon: FileText },
  { to: '/compliance',        label: 'Compliance',     Icon: FileText },
  { to: '/environmentals',    label: 'Environmentals', Icon: Leaf },
  { to: '/reports',           label: 'Reports',        Icon: FileText },
]

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

const OFFICE_SECTIONS: { key: string; label: string; Icon: React.ComponentType<{ size?: number; className?: string }>; groups: string[] }[] = [
  { key: 'front',  label: 'Front Office',  Icon: Briefcase,  groups: ['operations', 'prices', 'generation', 'gas', 'demand', 'system', 'renewables', 'storage'] },
  { key: 'middle', label: 'Middle Office', Icon: BarChart3,  groups: ['network', 'analytics', 'der', 'climate'] },
  { key: 'back',   label: 'Back Office',   Icon: Building2,  groups: ['retail', 'policy', 'emerging', 'other'] },
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
    '/deal-capture': 'prices', '/portfolio': 'prices',
    '/trade-blotter': 'prices', '/forward-curves': 'prices',
    '/risk-dashboard': 'prices',
    '/market-replay': 'operations',
    '/market-briefs': 'operations',
    '/bidding-dashboard': 'prices',
    '/battery-dispatch': 'storage',
    '/gas-market': 'gas',
    '/wem-dashboard': 'operations',
    '/compliance': 'policy',
    '/spot-forecast': 'prices',
    '/realtime-operations': 'operations',
    '/environmentals': 'renewables',
    '/reports': 'analytics',
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
    { key: 'trading', label: 'Trading', pattern: /trading|commodity|algo|merit|bidding|wholesale.*bidd|desk|deal|capture|blotter|portfolio/ },
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

// Error boundary for individual page components within CategoryPage
import React from 'react'
import GenericDashboardFallback from './components/GenericDashboardFallback'
class PageErrorBoundary extends React.Component<
  { children: React.ReactNode; pageName?: string },
  { hasError: boolean; error: Error | null }
> {
  constructor(props: { children: React.ReactNode; pageName?: string }) {
    super(props)
    this.state = { hasError: false, error: null }
  }
  static getDerivedStateFromError(error: Error) {
    return { hasError: true, error }
  }
  componentDidUpdate(prevProps: { pageName?: string }) {
    if (prevProps.pageName !== this.props.pageName) {
      this.setState({ hasError: false, error: null })
    }
  }
  render() {
    if (this.state.hasError) {
      const pageName = this.props.pageName || ''
      const apiPath = `/api${pageName}/dashboard`
      return (
        <GenericDashboardFallback
          apiPath={apiPath}
          errorMessage={this.state.error?.message}
          onRetry={() => this.setState({ hasError: false, error: null })}
        />
      )
    }
    return this.props.children
  }
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
        {ActiveComponent ? <PageErrorBoundary pageName={activePath ?? undefined}><ActiveComponent /></PageErrorBoundary> : <div className="p-6 text-gray-400 text-sm">Select a dashboard from the menu above</div>}
      </div>
    </div>
  )
}

function Sidebar() {
  const location = useLocation()
  const [expandedOffice, setExpandedOffice] = useState<string | null>('front')

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

  // Auto-expand the office section containing the active page
  useEffect(() => {
    if (activeCategoryKey) {
      const parentOffice = OFFICE_SECTIONS.find(o => o.groups.includes(activeCategoryKey))
      if (parentOffice) setExpandedOffice(parentOffice.key)
    }
  }, [activeCategoryKey])

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

        {/* Dashboard quick links */}
        <div className="px-3 pt-1 pb-0.5 text-[10px] font-semibold uppercase tracking-wider text-gray-500">Dashboards</div>
        {DASHBOARD_LINKS.map(({ to, label, Icon }) => (
          <NavLink
            key={to}
            to={to}
            className={({ isActive }) =>
              `flex items-center gap-2.5 pl-5 pr-3 py-1.5 rounded-md text-xs font-medium transition-colors ${
                isActive ? 'bg-white/10 text-white' : 'text-gray-400 hover:bg-white/5 hover:text-white'
              }`
            }
          >
            <Icon size={14} />
            {label}
          </NavLink>
        ))}

        <div className="h-px bg-white/10 my-2 mx-2" />

        {/* Category links — grouped by office section */}
        {OFFICE_SECTIONS.map(office => {
          const isExpanded = expandedOffice === office.key
          const officeGroups = GROUP_DEFS.filter(g => office.groups.includes(g.key))
          const hasActiveChild = officeGroups.some(g => activeCategoryKey === g.key)

          return (
            <div key={office.key} className="mt-1">
              <button
                onClick={() => setExpandedOffice(isExpanded ? null : office.key)}
                className={`flex items-center justify-between w-full px-3 py-2 rounded-md text-xs font-semibold uppercase tracking-wider transition-colors ${
                  hasActiveChild ? 'text-white' : 'text-gray-400 hover:text-gray-200'
                }`}
              >
                <span className="flex items-center gap-2">
                  <office.Icon size={14} />
                  <span>{office.label}</span>
                  {hasActiveChild && !isExpanded && (
                    <span className="w-1.5 h-1.5 rounded-full bg-blue-400 inline-block" />
                  )}
                </span>
                <ChevronDown size={14} className={`transition-transform duration-200 ${isExpanded ? 'rotate-180' : ''}`} />
              </button>

              <div className={`overflow-hidden transition-all duration-200 ${isExpanded ? 'max-h-96' : 'max-h-0'}`}>
                {officeGroups.map(group => {
                  const isActive = activeCategoryKey === group.key
                  return (
                    <NavLink
                      key={group.key}
                      to={`/cat/${group.key}`}
                      className={() =>
                        `flex items-center gap-2.5 pl-8 pr-3 py-1.5 rounded-md text-xs font-medium transition-colors ${
                          isActive ? 'bg-white/10 text-white' : 'text-gray-400 hover:bg-white/5 hover:text-white'
                        }`
                      }
                    >
                      <group.Icon size={14} />
                      {group.label}
                    </NavLink>
                  )
                })}
              </div>
            </div>
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
              <Route path="/live" element={<PageErrorBoundary pageName="/live"><LiveMarket /></PageErrorBoundary>} />
              <Route path="/forecasts" element={<PageErrorBoundary pageName="/forecasts"><Forecasts /></PageErrorBoundary>} />
              <Route path="/copilot"   element={<Copilot />}    />
              <Route path="/genie"     element={<Genie />}      />
              <Route path="/alerts" element={<PageErrorBoundary pageName="/alerts"><Alerts /></PageErrorBoundary>} />
              <Route path="/market-replay" element={<PageErrorBoundary pageName="/market-replay"><MarketReplay /></PageErrorBoundary>} />
              <Route path="/market-briefs" element={<PageErrorBoundary pageName="/market-briefs"><MarketBriefs /></PageErrorBoundary>} />
              <Route path="/monitoring" element={<PageErrorBoundary pageName="/monitoring"><Monitoring /></PageErrorBoundary>} />
              <Route path="/market-depth" element={<PageErrorBoundary pageName="/market-depth"><MarketDepth /></PageErrorBoundary>} />
              <Route path="/price-analysis" element={<PageErrorBoundary pageName="/price-analysis"><PriceAnalysis /></PageErrorBoundary>} />
              <Route path="/interconnectors" element={<PageErrorBoundary pageName="/interconnectors"><Interconnectors /></PageErrorBoundary>} />
              <Route path="/generator-fleet" element={<PageErrorBoundary pageName="/generator-fleet"><GeneratorFleet /></PageErrorBoundary>} />
              <Route path="/market-notices" element={<PageErrorBoundary pageName="/market-notices"><MarketNotices /></PageErrorBoundary>} />
              <Route path="/weather-demand" element={<PageErrorBoundary pageName="/weather-demand"><WeatherDemand /></PageErrorBoundary>} />
              <Route path="/bess" element={<PageErrorBoundary pageName="/bess"><BessAnalytics /></PageErrorBoundary>} />
              <Route path="/trading-desk" element={<PageErrorBoundary pageName="/trading-desk"><TradingDesk /></PageErrorBoundary>} />
              <Route path="/deal-capture" element={<PageErrorBoundary pageName="/deal-capture"><DealCapture /></PageErrorBoundary>} />
              <Route path="/portfolio" element={<PageErrorBoundary pageName="/portfolio"><PortfolioPage /></PageErrorBoundary>} />
              <Route path="/trade-blotter" element={<PageErrorBoundary pageName="/trade-blotter"><TradeBlotter /></PageErrorBoundary>} />
              <Route path="/forward-curves" element={<PageErrorBoundary pageName="/forward-curves"><ForwardCurves /></PageErrorBoundary>} />
              <Route path="/risk-dashboard" element={<PageErrorBoundary pageName="/risk-dashboard"><RiskDashboard /></PageErrorBoundary>} />
              <Route path="/bidding-dashboard" element={<PageErrorBoundary pageName="/bidding-dashboard"><BiddingDashboard /></PageErrorBoundary>} />
              <Route path="/battery-dispatch" element={<PageErrorBoundary pageName="/battery-dispatch"><BatteryDashboard /></PageErrorBoundary>} />
              <Route path="/gas-market" element={<PageErrorBoundary pageName="/gas-market"><GasMarketDashboard /></PageErrorBoundary>} />
              <Route path="/wem-dashboard" element={<PageErrorBoundary pageName="/wem-dashboard"><WemDashboard /></PageErrorBoundary>} />
              <Route path="/compliance" element={<PageErrorBoundary pageName="/compliance"><ComplianceDashboard /></PageErrorBoundary>} />
              <Route path="/environmentals" element={<PageErrorBoundary pageName="/environmentals"><EnvironmentalsDashboard /></PageErrorBoundary>} />
              <Route path="/reports" element={<PageErrorBoundary pageName="/reports"><ReportsLibrary /></PageErrorBoundary>} />
              <Route path="/sustainability" element={<PageErrorBoundary pageName="/sustainability"><Sustainability /></PageErrorBoundary>} />
              <Route path="/merit-order" element={<PageErrorBoundary pageName="/merit-order"><MeritOrder /></PageErrorBoundary>} />
              <Route path="/ml-dashboard" element={<PageErrorBoundary pageName="/ml-dashboard"><MlDashboardPage /></PageErrorBoundary>} />
              <Route path="/data-catalog" element={<PageErrorBoundary pageName="/data-catalog"><DataCatalog /></PageErrorBoundary>} />
              <Route path="/scenario" element={<PageErrorBoundary pageName="/scenario"><ScenarioAnalysis /></PageErrorBoundary>} />
              <Route path="/load-duration" element={<PageErrorBoundary pageName="/load-duration"><LoadDuration /></PageErrorBoundary>} />
              <Route path="/trends" element={<PageErrorBoundary pageName="/trends"><HistoricalTrends /></PageErrorBoundary>} />
              <Route path="/frequency" element={<PageErrorBoundary pageName="/frequency"><FrequencyAnalytics /></PageErrorBoundary>} />
              <Route path="/futures" element={<PageErrorBoundary pageName="/futures"><EnergyFutures /></PageErrorBoundary>} />
              <Route path="/registry" element={<PageErrorBoundary pageName="/registry"><ParticipantRegistry /></PageErrorBoundary>} />
              <Route path="/outages" element={<PageErrorBoundary pageName="/outages"><OutageSchedule /></PageErrorBoundary>} />
              <Route path="/der" element={<PageErrorBoundary pageName="/der"><DerDashboard /></PageErrorBoundary>} />
              <Route path="/gas" element={<PageErrorBoundary pageName="/gas"><GasMarket /></PageErrorBoundary>} />
              <Route path="/retail" element={<PageErrorBoundary pageName="/retail"><RetailMarket /></PageErrorBoundary>} />
              <Route path="/network" element={<PageErrorBoundary pageName="/network"><NetworkAnalytics /></PageErrorBoundary>} />
              <Route path="/rez" element={<PageErrorBoundary pageName="/rez"><RezInfrastructure /></PageErrorBoundary>} />
              <Route path="/curtailment" element={<PageErrorBoundary pageName="/curtailment"><CurtailmentAnalytics /></PageErrorBoundary>} />
              <Route path="/dsp" element={<PageErrorBoundary pageName="/dsp"><DemandResponse /></PageErrorBoundary>} />
              <Route path="/security" element={<PageErrorBoundary pageName="/security"><SystemSecurity /></PageErrorBoundary>} />
              <Route path="/bidding" element={<PageErrorBoundary pageName="/bidding"><BiddingAnalytics /></PageErrorBoundary>} />
              <Route path="/nem-events" element={<PageErrorBoundary pageName="/nem-events"><NemEvents /></PageErrorBoundary>} />
              <Route path="/fcas-market" element={<PageErrorBoundary pageName="/fcas-market"><FcasMarket /></PageErrorBoundary>} />
              <Route path="/battery-econ" element={<PageErrorBoundary pageName="/battery-econ"><BatteryEconomics /></PageErrorBoundary>} />
              <Route path="/battery-economics" element={<PageErrorBoundary pageName="/battery-economics"><BatteryEconomics /></PageErrorBoundary>} />
              <Route path="/settlement" element={<PageErrorBoundary pageName="/settlement"><NemSettlement /></PageErrorBoundary>} />
              <Route path="/carbon" element={<PageErrorBoundary pageName="/carbon"><CarbonAnalytics /></PageErrorBoundary>} />
              <Route path="/hedging" element={<PageErrorBoundary pageName="/hedging"><HedgingAnalytics /></PageErrorBoundary>} />
              <Route path="/hydro" element={<PageErrorBoundary pageName="/hydro"><HydroStorage /></PageErrorBoundary>} />
              <Route path="/market-power" element={<PageErrorBoundary pageName="/market-power"><MarketPower /></PageErrorBoundary>} />
              <Route path="/pasa" element={<PageErrorBoundary pageName="/pasa"><PasaAnalytics /></PageErrorBoundary>} />
              <Route path="/sra" element={<PageErrorBoundary pageName="/sra"><SraAuction /></PageErrorBoundary>} />
              <Route path="/ppa" element={<PageErrorBoundary pageName="/ppa"><PpaMarket /></PageErrorBoundary>} />
              <Route path="/dispatch" element={<PageErrorBoundary pageName="/dispatch"><DispatchAccuracy /></PageErrorBoundary>} />
              <Route path="/regulatory" element={<PageErrorBoundary pageName="/regulatory"><RegulatoryTracker /></PageErrorBoundary>} />
              <Route path="/isp-tracker" element={<PageErrorBoundary pageName="/isp-tracker"><IspTracker /></PageErrorBoundary>} />
              <Route path="/solar-ev" element={<PageErrorBoundary pageName="/solar-ev"><SolarEvAnalytics /></PageErrorBoundary>} />
              <Route path="/lrmc" element={<PageErrorBoundary pageName="/lrmc"><LrmcAnalytics /></PageErrorBoundary>} />
              <Route path="/constraints" element={<PageErrorBoundary pageName="/constraints"><NetworkConstraints /></PageErrorBoundary>} />
              <Route path="/price-setter" element={<PageErrorBoundary pageName="/price-setter"><PriceSetterAnalytics /></PageErrorBoundary>} />
              <Route path="/tariff" element={<PageErrorBoundary pageName="/tariff"><TariffAnalytics /></PageErrorBoundary>} />
              <Route path="/grid-mod" element={<PageErrorBoundary pageName="/grid-mod"><GridModernisation /></PageErrorBoundary>} />
              <Route path="/spot-cap" element={<PageErrorBoundary pageName="/spot-cap"><SpotCapAnalytics /></PageErrorBoundary>} />
              <Route path="/causer-pays" element={<PageErrorBoundary pageName="/causer-pays"><CauserPays /></PageErrorBoundary>} />
              <Route path="/wem" element={<PageErrorBoundary pageName="/wem"><WemOverview /></PageErrorBoundary>} />
              <Route path="/inertia" element={<PageErrorBoundary pageName="/inertia"><InertiaAnalytics /></PageErrorBoundary>} />
              <Route path="/tnsp" element={<PageErrorBoundary pageName="/tnsp"><TnspAnalytics /></PageErrorBoundary>} />
              <Route path="/surveillance" element={<PageErrorBoundary pageName="/surveillance"><MarketSurveillance /></PageErrorBoundary>} />
              <Route path="/hydrogen" element={<PageErrorBoundary pageName="/hydrogen"><HydrogenAnalytics /></PageErrorBoundary>} />
              <Route path="/offshore-wind" element={<PageErrorBoundary pageName="/offshore-wind"><OffshoreWind /></PageErrorBoundary>} />
              <Route path="/offshore-wind-pipeline" element={<PageErrorBoundary pageName="/offshore-wind-pipeline"><OffshoreWindPipeline /></PageErrorBoundary>} />
              <Route path="/cer" element={<PageErrorBoundary pageName="/cer"><CerDashboard /></PageErrorBoundary>} />
              <Route path="/phes" element={<PageErrorBoundary pageName="/phes"><PhesAnalytics /></PageErrorBoundary>} />
              <Route path="/safeguard" element={<PageErrorBoundary pageName="/safeguard"><SafeguardAnalytics /></PageErrorBoundary>} />
              <Route path="/transmission" element={<PageErrorBoundary pageName="/transmission"><TransmissionProjects /></PageErrorBoundary>} />
              <Route path="/dnsp" element={<PageErrorBoundary pageName="/dnsp"><DnspAnalytics /></PageErrorBoundary>} />
              <Route path="/vpp" element={<PageErrorBoundary pageName="/vpp"><VppDashboard /></PageErrorBoundary>} />
              <Route path="/reform" element={<PageErrorBoundary pageName="/reform"><MarketReformTracker /></PageErrorBoundary>} />
              <Route path="/tuos" element={<PageErrorBoundary pageName="/tuos"><TuosAnalytics /></PageErrorBoundary>} />
              <Route path="/carbon-registry" element={<PageErrorBoundary pageName="/carbon-registry"><CarbonRegistry /></PageErrorBoundary>} />
              <Route path="/ev" element={<PageErrorBoundary pageName="/ev"><EvCharging /></PageErrorBoundary>} />
              <Route path="/storage" element={<PageErrorBoundary pageName="/storage"><StorageArbitrage /></PageErrorBoundary>} />
              <Route path="/demand-forecast" element={<PageErrorBoundary pageName="/demand-forecast"><DemandForecastAnalytics /></PageErrorBoundary>} />
              <Route path="/rez-development" element={<PageErrorBoundary pageName="/rez-development"><RenewableEnergyZoneDevelopmentAnalytics /></PageErrorBoundary>} />
              <Route path="/congestion" element={<PageErrorBoundary pageName="/congestion"><CongestionAnalytics /></PageErrorBoundary>} />
              <Route path="/equity" element={<PageErrorBoundary pageName="/equity"><EnergyEquity /></PageErrorBoundary>} />
              <Route path="/demand-response" element={<PageErrorBoundary pageName="/demand-response"><DemandResponseAnalytics /></PageErrorBoundary>} />
              <Route path="/btm" element={<PageErrorBoundary pageName="/btm"><BehindTheMeter /></PageErrorBoundary>} />
              <Route path="/rab" element={<PageErrorBoundary pageName="/rab"><RabAnalytics /></PageErrorBoundary>} />
              <Route path="/realtime" element={<PageErrorBoundary pageName="/realtime"><NemRealTimeDashboard /></PageErrorBoundary>} />
              <Route path="/rit" element={<PageErrorBoundary pageName="/rit"><RitAnalytics /></PageErrorBoundary>} />
              <Route path="/forward-curve" element={<PageErrorBoundary pageName="/forward-curve"><ForwardCurveAnalytics /></PageErrorBoundary>} />
              <Route path="/coal-retirement" element={<PageErrorBoundary pageName="/coal-retirement"><CoalRetirement /></PageErrorBoundary>} />
              <Route path="/gas-gen" element={<PageErrorBoundary pageName="/gas-gen"><GasGenEconomics /></PageErrorBoundary>} />
              <Route path="/consumer-protection" element={<PageErrorBoundary pageName="/consumer-protection"><ConsumerProtection /></PageErrorBoundary>} />
              <Route path="/efor" element={<PageErrorBoundary pageName="/efor"><GeneratorAvailability /></PageErrorBoundary>} />
              <Route path="/climate-risk" element={<PageErrorBoundary pageName="/climate-risk"><ClimateRiskAnalytics /></PageErrorBoundary>} />
              <Route path="/smart-grid" element={<PageErrorBoundary pageName="/smart-grid"><SmartGridAnalytics /></PageErrorBoundary>} />
              <Route path="/minimum-demand" element={<PageErrorBoundary pageName="/minimum-demand"><MinimumDemandAnalytics /></PageErrorBoundary>} />
              <Route path="/market-events" element={<PageErrorBoundary pageName="/market-events"><MarketEventsAnalysis /></PageErrorBoundary>} />
              <Route path="/battery-tech" element={<PageErrorBoundary pageName="/battery-tech"><BatteryTechAnalytics /></PageErrorBoundary>} />
              <Route path="/community-energy" element={<PageErrorBoundary pageName="/community-energy"><CommunityEnergy /></PageErrorBoundary>} />
              <Route path="/asset-management" element={<PageErrorBoundary pageName="/asset-management"><AssetManagement /></PageErrorBoundary>} />
              <Route path="/decarbonization" element={<PageErrorBoundary pageName="/decarbonization"><DecarbonizationPathway /></PageErrorBoundary>} />
              <Route path="/nuclear-ldes" element={<PageErrorBoundary pageName="/nuclear-ldes"><NuclearLongDuration /></PageErrorBoundary>} />
              <Route path="/nuclear-energy" element={<PageErrorBoundary pageName="/nuclear-energy"><NuclearEnergyAnalytics /></PageErrorBoundary>} />
              <Route path="/bidding-behaviour" element={<PageErrorBoundary pageName="/bidding-behaviour"><BiddingBehaviour /></PageErrorBoundary>} />
              <Route path="/energy-poverty" element={<PageErrorBoundary pageName="/energy-poverty"><EnergyPoverty /></PageErrorBoundary>} />
              <Route path="/spot-forecast" element={<PageErrorBoundary pageName="/spot-forecast"><SpotForecastDashboard /></PageErrorBoundary>} />
              <Route path="/hydrogen-economy" element={<PageErrorBoundary pageName="/hydrogen-economy"><HydrogenEconomy /></PageErrorBoundary>} />
              <Route path="/hydrogen-economy-analytics" element={<PageErrorBoundary pageName="/hydrogen-economy-analytics"><HydrogenEconomyAnalytics /></PageErrorBoundary>} />
              <Route path="/carbon-credit" element={<PageErrorBoundary pageName="/carbon-credit"><CarbonCreditMarket /></PageErrorBoundary>} />
              <Route path="/grid-resilience" element={<PageErrorBoundary pageName="/grid-resilience"><GridResilience /></PageErrorBoundary>} />
              <Route path="/ev-fleet" element={<PageErrorBoundary pageName="/ev-fleet"><EvFleetCharging /></PageErrorBoundary>} />
              <Route path="/rec-market" element={<PageErrorBoundary pageName="/rec-market"><RecMarket /></PageErrorBoundary>} />
              <Route path="/transmission-congestion" element={<PageErrorBoundary pageName="/transmission-congestion"><TransmissionCongestion /></PageErrorBoundary>} />
              <Route path="/derms-orchestration" element={<PageErrorBoundary pageName="/derms-orchestration"><DermsOrchestration /></PageErrorBoundary>} />
              <Route path="/market-design" element={<PageErrorBoundary pageName="/market-design"><MarketDesignReform /></PageErrorBoundary>} />
              <Route path="/rez-capacity" element={<PageErrorBoundary pageName="/rez-capacity"><RezCapacityTracking /></PageErrorBoundary>} />
              <Route path="/retail-offer-comparison" element={<PageErrorBoundary pageName="/retail-offer-comparison"><RetailOfferComparison /></PageErrorBoundary>} />
              <Route path="/system-operator" element={<PageErrorBoundary pageName="/system-operator"><SystemOperatorActions /></PageErrorBoundary>} />
              <Route path="/network-tariff-reform" element={<PageErrorBoundary pageName="/network-tariff-reform"><NetworkTariffReform /></PageErrorBoundary>} />
              <Route path="/spike-analysis" element={<PageErrorBoundary pageName="/spike-analysis"><PriceSpikeAnalysis /></PageErrorBoundary>} />
              <Route path="/storage-revenue-stack" element={<PageErrorBoundary pageName="/storage-revenue-stack"><StorageRevenueStack /></PageErrorBoundary>} />
              <Route path="/solar-resource" element={<PageErrorBoundary pageName="/solar-resource"><SolarResourceAnalytics /></PageErrorBoundary>} />
              <Route path="/futures-market-risk" element={<PageErrorBoundary pageName="/futures-market-risk"><FuturesMarketRisk /></PageErrorBoundary>} />
              <Route path="/wind-resource" element={<PageErrorBoundary pageName="/wind-resource"><WindResourceAnalytics /></PageErrorBoundary>} />
              <Route path="/corporate-ppa-market" element={<PageErrorBoundary pageName="/corporate-ppa-market"><CorporatePpaMarket /></PageErrorBoundary>} />
              <Route path="/microgrid-raps" element={<PageErrorBoundary pageName="/microgrid-raps"><MicrogridRaps /></PageErrorBoundary>} />
              <Route path="/market-liquidity" element={<PageErrorBoundary pageName="/market-liquidity"><MarketLiquidity /></PageErrorBoundary>} />
              <Route path="/thermal-efficiency" element={<PageErrorBoundary pageName="/thermal-efficiency"><ThermalEfficiency /></PageErrorBoundary>} />
              <Route path="/industrial-demand-flex" element={<PageErrorBoundary pageName="/industrial-demand-flex"><IndustrialDemandFlex /></PageErrorBoundary>} />
              <Route path="/storage-lca" element={<PageErrorBoundary pageName="/storage-lca"><StorageLca /></PageErrorBoundary>} />
              <Route path="/interconnector-flow-analytics" element={<PageErrorBoundary pageName="/interconnector-flow-analytics"><InterconnectorFlowAnalytics /></PageErrorBoundary>} />
              <Route path="/isp-progress" element={<PageErrorBoundary pageName="/isp-progress"><IspProgressTracker /></PageErrorBoundary>} />
              <Route path="/firming-technology-economics" element={<PageErrorBoundary pageName="/firming-technology-economics"><FirmingTechnologyEconomics /></PageErrorBoundary>} />
              <Route path="/demand-forecasting-models" element={<PageErrorBoundary pageName="/demand-forecasting-models"><DemandForecastingModels /></PageErrorBoundary>} />
              <Route path="/market-stress-testing" element={<PageErrorBoundary pageName="/market-stress-testing"><MarketStressTesting /></PageErrorBoundary>} />
              <Route path="/capacity-investment-signals" element={<PageErrorBoundary pageName="/capacity-investment-signals"><CapacityInvestmentSignals /></PageErrorBoundary>} />
              <Route path="/frequency-control-analytics" element={<PageErrorBoundary pageName="/frequency-control-analytics"><FrequencyControlAnalytics /></PageErrorBoundary>} />
              <Route path="/rec-certificate-tracking" element={<PageErrorBoundary pageName="/rec-certificate-tracking"><RecCertificateTracking /></PageErrorBoundary>} />
              <Route path="/spot-market-depth" element={<PageErrorBoundary pageName="/spot-market-depth"><SpotMarketDepthAnalytics /></PageErrorBoundary>} />
              <Route path="/storage-tech-roadmap" element={<PageErrorBoundary pageName="/storage-tech-roadmap"><StorageTechRoadmap /></PageErrorBoundary>} />
              <Route path="/renewable-integration-cost" element={<PageErrorBoundary pageName="/renewable-integration-cost"><RenewableIntegrationCost /></PageErrorBoundary>} />
              <Route path="/planned-outage-analytics" element={<PageErrorBoundary pageName="/planned-outage-analytics"><PlannedOutageAnalytics /></PageErrorBoundary>} />
              <Route path="/market-share-tracker" element={<PageErrorBoundary pageName="/market-share-tracker"><MarketShareTracker /></PageErrorBoundary>} />
              <Route path="/volatility-regime-analytics" element={<PageErrorBoundary pageName="/volatility-regime-analytics"><VolatilityRegimeAnalytics /></PageErrorBoundary>} />
              <Route path="/black-start-capability" element={<PageErrorBoundary pageName="/black-start-capability"><BlackStartCapability /></PageErrorBoundary>} />
              <Route path="/ancillary-services-cost" element={<PageErrorBoundary pageName="/ancillary-services-cost"><AncillaryServicesCost /></PageErrorBoundary>} />
              <Route path="/cbam-trade-analytics" element={<PageErrorBoundary pageName="/cbam-trade-analytics"><CbamTradeAnalytics /></PageErrorBoundary>} />
              <Route path="/congestion-revenue-analytics" element={<PageErrorBoundary pageName="/congestion-revenue-analytics"><CongestionRevenueAnalytics /></PageErrorBoundary>} />
              <Route path="/climate-physical-risk" element={<PageErrorBoundary pageName="/climate-physical-risk"><ClimatePhysicalRisk /></PageErrorBoundary>} />
              <Route path="/energy-affordability" element={<PageErrorBoundary pageName="/energy-affordability"><EnergyAffordabilityAnalytics /></PageErrorBoundary>} />
              <Route path="/electrification-analytics" element={<PageErrorBoundary pageName="/electrification-analytics"><ElectrificationAnalytics /></PageErrorBoundary>} />
              <Route path="/electricity-export-infra" element={<PageErrorBoundary pageName="/electricity-export-infra"><ElectricityExportInfra /></PageErrorBoundary>} />
              <Route path="/electricity-export-economics" element={<PageErrorBoundary pageName="/electricity-export-economics"><ElectricityExportEconomicsAnalytics /></PageErrorBoundary>} />
              <Route path="/ldes-economics" element={<PageErrorBoundary pageName="/ldes-economics"><LdesEconomicsAnalytics /></PageErrorBoundary>} />
              <Route path="/gas-transition-analytics" element={<PageErrorBoundary pageName="/gas-transition-analytics"><GasTransitionAnalytics /></PageErrorBoundary>} />
              <Route path="/prosumer-analytics" element={<PageErrorBoundary pageName="/prosumer-analytics"><ProsumerAnalytics /></PageErrorBoundary>} />
              <Route path="/tnsp-analytics" element={<PageErrorBoundary pageName="/tnsp-analytics"><TnspAnalytics /></PageErrorBoundary>} />
              <Route path="/dnsp-analytics" element={<PageErrorBoundary pageName="/dnsp-analytics"><DnspPerformanceAnalytics /></PageErrorBoundary>} />
              <Route path="/reliability-standard-analytics" element={<PageErrorBoundary pageName="/reliability-standard-analytics"><ReliabilityStandardAnalytics /></PageErrorBoundary>} />
              <Route path="/storage-optimisation-analytics" element={<PageErrorBoundary pageName="/storage-optimisation-analytics"><StorageOptimisationAnalytics /></PageErrorBoundary>} />
              <Route path="/settlement-analytics" element={<PageErrorBoundary pageName="/settlement-analytics"><SettlementAnalytics /></PageErrorBoundary>} />
              <Route path="/realtime-operations" element={<PageErrorBoundary pageName="/realtime-operations"><RealtimeOperationsDashboard /></PageErrorBoundary>} />
              <Route path="/renewable-auction" element={<PageErrorBoundary pageName="/renewable-auction"><RenewableAuctionAnalytics /></PageErrorBoundary>} />
              <Route path="/voll-analytics" element={<PageErrorBoundary pageName="/voll-analytics"><VollAnalytics /></PageErrorBoundary>} />
              <Route path="/demand-flexibility-analytics" element={<PageErrorBoundary pageName="/demand-flexibility-analytics"><DemandFlexibilityAnalytics /></PageErrorBoundary>} />
              <Route path="/futures-price-discovery" element={<PageErrorBoundary pageName="/futures-price-discovery"><FuturesPriceDiscovery /></PageErrorBoundary>} />
              <Route path="/electricity-price-index" element={<PageErrorBoundary pageName="/electricity-price-index"><ElectricityPriceIndex /></PageErrorBoundary>} />
              <Route path="/interconnector-upgrade-analytics" element={<PageErrorBoundary pageName="/interconnector-upgrade-analytics"><InterconnectorUpgradeAnalytics /></PageErrorBoundary>} />
              <Route path="/mlf-analytics" element={<PageErrorBoundary pageName="/mlf-analytics"><MlfAnalytics /></PageErrorBoundary>} />
              <Route path="/csp-analytics" element={<PageErrorBoundary pageName="/csp-analytics"><CspAnalytics /></PageErrorBoundary>} />
              <Route path="/carbon-intensity-analytics" element={<PageErrorBoundary pageName="/carbon-intensity-analytics"><CarbonIntensityAnalytics /></PageErrorBoundary>} />
              <Route path="/network-tariff-reform-analytics" element={<PageErrorBoundary pageName="/network-tariff-reform-analytics"><NetworkTariffReformAnalytics /></PageErrorBoundary>} />
              <Route path="/tariff-cross-subsidy" element={<PageErrorBoundary pageName="/tariff-cross-subsidy"><TariffCrossSubsidyAnalytics /></PageErrorBoundary>} />
              <Route path="/ai-digital-twin-analytics" element={<PageErrorBoundary pageName="/ai-digital-twin-analytics"><AiDigitalTwinAnalytics /></PageErrorBoundary>} />
              <Route path="/esoo-adequacy-analytics" element={<PageErrorBoundary pageName="/esoo-adequacy-analytics"><EsooAdequacyAnalytics /></PageErrorBoundary>} />
              <Route path="/social-licence-analytics" element={<PageErrorBoundary pageName="/social-licence-analytics"><SocialLicenceAnalytics /></PageErrorBoundary>} />
              <Route path="/electricity-options" element={<PageErrorBoundary pageName="/electricity-options"><ElectricityOptionsAnalytics /></PageErrorBoundary>} />
              <Route path="/grid-forming-inverter" element={<PageErrorBoundary pageName="/grid-forming-inverter"><GridFormingInverterAnalytics /></PageErrorBoundary>} />
              <Route path="/capacity-mechanism" element={<PageErrorBoundary pageName="/capacity-mechanism"><CapacityMechanismAnalytics /></PageErrorBoundary>} />
              <Route path="/demand-forecast-accuracy" element={<PageErrorBoundary pageName="/demand-forecast-accuracy"><DemandForecastAccuracyAnalytics /></PageErrorBoundary>} />
              <Route path="/transmission-investment" element={<PageErrorBoundary pageName="/transmission-investment"><TransmissionInvestmentAnalytics /></PageErrorBoundary>} />
              <Route path="/rez-progress" element={<PageErrorBoundary pageName="/rez-progress"><RezProgressAnalytics /></PageErrorBoundary>} />
              <Route path="/storage-revenue" element={<PageErrorBoundary pageName="/storage-revenue"><StorageRevenueAnalytics /></PageErrorBoundary>} />
              <Route path="/carbon-price-pathway" element={<PageErrorBoundary pageName="/carbon-price-pathway"><CarbonPricePathwayAnalytics /></PageErrorBoundary>} />
              <Route path="/spot-price-forecast" element={<PageErrorBoundary pageName="/spot-price-forecast"><SpotPriceForecastAnalytics /></PageErrorBoundary>} />
              <Route path="/ancillary-cost-allocation" element={<PageErrorBoundary pageName="/ancillary-cost-allocation"><AncillaryCostAllocationAnalytics /></PageErrorBoundary>} />
              <Route path="/wholesale-liquidity" element={<PageErrorBoundary pageName="/wholesale-liquidity"><MarketLiquidityAnalytics /></PageErrorBoundary>} />
              <Route path="/generator-retirement" element={<PageErrorBoundary pageName="/generator-retirement"><GeneratorRetirementAnalytics /></PageErrorBoundary>} />
              <Route path="/consumer-hardship" element={<PageErrorBoundary pageName="/consumer-hardship"><ConsumerHardshipAnalytics /></PageErrorBoundary>} />
              <Route path="/dsr-aggregator" element={<PageErrorBoundary pageName="/dsr-aggregator"><DsrAggregatorAnalytics /></PageErrorBoundary>} />
              <Route path="/power-system-events" element={<PageErrorBoundary pageName="/power-system-events"><PowerSystemEventsAnalytics /></PageErrorBoundary>} />
              <Route path="/merchant-renewable" element={<PageErrorBoundary pageName="/merchant-renewable"><MerchantRenewableAnalytics /></PageErrorBoundary>} />
              <Route path="/retailer-competition" element={<PageErrorBoundary pageName="/retailer-competition"><RetailerCompetitionAnalytics /></PageErrorBoundary>} />
              <Route path="/storage-cost-curves" element={<PageErrorBoundary pageName="/storage-cost-curves"><StorageCostCurvesAnalytics /></PageErrorBoundary>} />
              <Route path="/extreme-weather-resilience" element={<PageErrorBoundary pageName="/extreme-weather-resilience"><ExtremeWeatherResilienceAnalytics /></PageErrorBoundary>} />
              <Route path="/spot-price-volatility-regime" element={<PageErrorBoundary pageName="/spot-price-volatility-regime"><SpotPriceVolatilityRegimeAnalytics /></PageErrorBoundary>} />
              <Route path="/industrial-electrification" element={<PageErrorBoundary pageName="/industrial-electrification"><IndustrialElectrificationAnalytics /></PageErrorBoundary>} />
              <Route path="/offshore-wind-dev-analytics" element={<PageErrorBoundary pageName="/offshore-wind-dev-analytics"><OffshoreWindDevAnalytics /></PageErrorBoundary>} />
              <Route path="/pumped-hydro-resource-assessment" element={<PageErrorBoundary pageName="/pumped-hydro-resource-assessment"><PumpedHydroResourceAssessmentAnalytics /></PageErrorBoundary>} />
              <Route path="/frequency-control-performance" element={<PageErrorBoundary pageName="/frequency-control-performance"><FrequencyControlPerformanceAnalytics /></PageErrorBoundary>} />
              <Route path="/cost-reflective-tariff-reform" element={<PageErrorBoundary pageName="/cost-reflective-tariff-reform"><CostReflectiveTariffReformAnalytics /></PageErrorBoundary>} />
              <Route path="/ev-fleet-grid-impact" element={<PageErrorBoundary pageName="/ev-fleet-grid-impact"><EVFleetGridImpactAnalytics /></PageErrorBoundary>} />
              <Route path="/nem-market-microstructure" element={<PageErrorBoundary pageName="/nem-market-microstructure"><NEMMarketMicrostructureAnalytics /></PageErrorBoundary>} />
              <Route path="/rooftop-solar-grid" element={<PageErrorBoundary pageName="/rooftop-solar-grid"><RooftopSolarGridAnalytics /></PageErrorBoundary>} />
              <Route path="/rec-market-analytics" element={<PageErrorBoundary pageName="/rec-market-analytics"><RECMarketAnalytics /></PageErrorBoundary>} />
              <Route path="/energy-poverty-analytics" element={<PageErrorBoundary pageName="/energy-poverty-analytics"><EnergyPovertyAnalytics /></PageErrorBoundary>} />
              <Route path="/hedge-effectiveness" element={<PageErrorBoundary pageName="/hedge-effectiveness"><HedgeEffectivenessAnalytics /></PageErrorBoundary>} />
              <Route path="/cbam-trade-exposure" element={<PageErrorBoundary pageName="/cbam-trade-exposure"><CBAMTradeExposureAnalytics /></PageErrorBoundary>} />
              <Route path="/demand-response-programs" element={<PageErrorBoundary pageName="/demand-response-programs"><DemandResponseProgramAnalytics /></PageErrorBoundary>} />
              <Route path="/interconnector-congestion" element={<PageErrorBoundary pageName="/interconnector-congestion"><InterconnectorCongestionAnalytics /></PageErrorBoundary>} />
              <Route path="/ppa-market" element={<PageErrorBoundary pageName="/ppa-market"><PPAMarketAnalytics /></PageErrorBoundary>} />
              <Route path="/ppa-structuring" element={<PageErrorBoundary pageName="/ppa-structuring"><PPAStructuringAnalytics /></PageErrorBoundary>} />
              <Route path="/battery-dispatch-strategy" element={<PageErrorBoundary pageName="/battery-dispatch-strategy"><BatteryDispatchStrategyAnalytics /></PageErrorBoundary>} />
              <Route path="/generation-mix-transition" element={<PageErrorBoundary pageName="/generation-mix-transition"><GenerationMixTransitionAnalytics /></PageErrorBoundary>} />
              <Route path="/storage-duration-economics" element={<PageErrorBoundary pageName="/storage-duration-economics"><StorageDurationEconomicsAnalytics /></PageErrorBoundary>} />
              <Route path="/ancillary-market-depth" element={<PageErrorBoundary pageName="/ancillary-market-depth"><AncillaryServicesMarketDepthAnalytics /></PageErrorBoundary>} />
              <Route path="/sra-analytics" element={<PageErrorBoundary pageName="/sra-analytics"><SRAAnalyticsPage /></PageErrorBoundary>} />
              <Route path="/spot-market-stress" element={<PageErrorBoundary pageName="/spot-market-stress"><SpotMarketStressAnalytics /></PageErrorBoundary>} />
              <Route path="/electricity-workforce" element={<PageErrorBoundary pageName="/electricity-workforce"><ElectricityWorkforceAnalytics /></PageErrorBoundary>} />
              <Route path="/rez-transmission" element={<PageErrorBoundary pageName="/rez-transmission"><REZTransmissionAnalytics /></PageErrorBoundary>} />
              <Route path="/network-regulatory-framework" element={<PageErrorBoundary pageName="/network-regulatory-framework"><NetworkRegulatoryFrameworkAnalytics /></PageErrorBoundary>} />
              <Route path="/price-model-comparison" element={<PageErrorBoundary pageName="/price-model-comparison"><PriceModelComparisonAnalytics /></PageErrorBoundary>} />
              <Route path="/gas-electricity-nexus" element={<PageErrorBoundary pageName="/gas-electricity-nexus"><GasElectricityNexusAnalytics /></PageErrorBoundary>} />
              <Route path="/bidding-compliance" element={<PageErrorBoundary pageName="/bidding-compliance"><BiddingComplianceAnalytics /></PageErrorBoundary>} />
              <Route path="/community-energy-analytics" element={<PageErrorBoundary pageName="/community-energy-analytics"><CommunityEnergyAnalytics /></PageErrorBoundary>} />
              <Route path="/grid-cybersecurity" element={<PageErrorBoundary pageName="/grid-cybersecurity"><GridCybersecurityAnalytics /></PageErrorBoundary>} />
              <Route path="/market-participant-financial" element={<PageErrorBoundary pageName="/market-participant-financial"><MarketParticipantFinancialAnalytics /></PageErrorBoundary>} />
              <Route path="/digital-transformation" element={<PageErrorBoundary pageName="/digital-transformation"><DigitalTransformationAnalytics /></PageErrorBoundary>} />
              <Route path="/energy-transition-finance" element={<PageErrorBoundary pageName="/energy-transition-finance"><EnergyTransitionFinanceAnalytics /></PageErrorBoundary>} />
              <Route path="/negative-price-events" element={<PageErrorBoundary pageName="/negative-price-events"><NegativePriceEventAnalytics /></PageErrorBoundary>} />
              <Route path="/cer-orchestration" element={<PageErrorBoundary pageName="/cer-orchestration"><CEROrchestrationAnalytics /></PageErrorBoundary>} />
              <Route path="/system-load-balancing" element={<PageErrorBoundary pageName="/system-load-balancing"><SystemLoadBalancingAnalytics /></PageErrorBoundary>} />
              <Route path="/carbon-accounting" element={<PageErrorBoundary pageName="/carbon-accounting"><CarbonAccountingAnalytics /></PageErrorBoundary>} />
              <Route path="/wholesale-bidding-strategy" element={<PageErrorBoundary pageName="/wholesale-bidding-strategy"><WholesaleBiddingStrategyAnalytics /></PageErrorBoundary>} />
              <Route path="/ldes-analytics" element={<PageErrorBoundary pageName="/ldes-analytics"><LDESAnalytics /></PageErrorBoundary>} />
              <Route path="/emergency-management" element={<PageErrorBoundary pageName="/emergency-management"><EmergencyManagementAnalytics /></PageErrorBoundary>} />
              <Route path="/consumer-switching-retail-churn" element={<PageErrorBoundary pageName="/consumer-switching-retail-churn"><ConsumerSwitchingRetailChurnAnalytics /></PageErrorBoundary>} />
              <Route path="/solar-thermal-csp" element={<PageErrorBoundary pageName="/solar-thermal-csp"><SolarThermalCSPAnalytics /></PageErrorBoundary>} />
              <Route path="/nem-post-reform-market-design" element={<PageErrorBoundary pageName="/nem-post-reform-market-design"><NEMPostReformMarketDesignAnalytics /></PageErrorBoundary>} />
              <Route path="/electricity-price-forecasting-models" element={<PageErrorBoundary pageName="/electricity-price-forecasting-models"><ElectricityPriceForecastingModelAnalytics /></PageErrorBoundary>} />
              <Route path="/large-industrial-demand" element={<PageErrorBoundary pageName="/large-industrial-demand"><LargeIndustrialDemandAnalytics /></PageErrorBoundary>} />
              <Route path="/network-investment-pipeline" element={<PageErrorBoundary pageName="/network-investment-pipeline"><NetworkInvestmentPipelineAnalytics /></PageErrorBoundary>} />
              <Route path="/nem-demand-forecast" element={<PageErrorBoundary pageName="/nem-demand-forecast"><NEMDemandForecastAnalytics /></PageErrorBoundary>} />
              <Route path="/hydrogen-fuel-cell-vehicles" element={<PageErrorBoundary pageName="/hydrogen-fuel-cell-vehicles"><HydrogenFuelCellVehicleAnalytics /></PageErrorBoundary>} />
              <Route path="/spot-price-spike-prediction" element={<PageErrorBoundary pageName="/spot-price-spike-prediction"><SpotPriceSpikePredictionAnalytics /></PageErrorBoundary>} />
              <Route path="/grid-edge-technology" element={<PageErrorBoundary pageName="/grid-edge-technology"><GridEdgeTechnologyAnalytics /></PageErrorBoundary>} />
              <Route path="/bess-degradation" element={<PageErrorBoundary pageName="/bess-degradation"><EnergyStorageDegradationAnalytics /></PageErrorBoundary>} />
              <Route path="/clean-hydrogen-production-cost" element={<PageErrorBoundary pageName="/clean-hydrogen-production-cost"><CleanHydrogenProductionCostAnalytics /></PageErrorBoundary>} />
              <Route path="/ancillary-services-procurement" element={<PageErrorBoundary pageName="/ancillary-services-procurement"><AncillaryServicesProcurementAnalytics /></PageErrorBoundary>} />
              <Route path="/rez-connection-queue" element={<PageErrorBoundary pageName="/rez-connection-queue"><REZConnectionQueueAnalytics /></PageErrorBoundary>} />
              <Route path="/australian-carbon-policy" element={<PageErrorBoundary pageName="/australian-carbon-policy"><AustralianCarbonPolicyAnalytics /></PageErrorBoundary>} />
              <Route path="/market-design-simulation" element={<PageErrorBoundary pageName="/market-design-simulation"><MarketDesignSimulationAnalytics /></PageErrorBoundary>} />
              <Route path="/power-system-stability" element={<PageErrorBoundary pageName="/power-system-stability"><PowerSystemStabilityAnalytics /></PageErrorBoundary>} />
              <Route path="/energy-retail-competition" element={<PageErrorBoundary pageName="/energy-retail-competition"><EnergyRetailCompetitionAnalytics /></PageErrorBoundary>} />
              <Route path="/clean-energy-finance" element={<PageErrorBoundary pageName="/clean-energy-finance"><CleanEnergyFinanceAnalytics /></PageErrorBoundary>} />
              <Route path="/nuclear-energy-economics" element={<PageErrorBoundary pageName="/nuclear-energy-economics"><NuclearEnergyEconomicsAnalytics /></PageErrorBoundary>} />
              <Route path="/behind-meter-commercial" element={<PageErrorBoundary pageName="/behind-meter-commercial"><BehindMeterCommercialAnalytics /></PageErrorBoundary>} />
              <Route path="/capacity-investment-scheme" element={<PageErrorBoundary pageName="/capacity-investment-scheme"><CapacityInvestmentSchemeAnalytics /></PageErrorBoundary>} />
              <Route path="/demand-flexibility-market" element={<PageErrorBoundary pageName="/demand-flexibility-market"><DemandFlexibilityMarketAnalytics /></PageErrorBoundary>} />
              <Route path="/energy-asset-life-extension" element={<PageErrorBoundary pageName="/energy-asset-life-extension"><EnergyAssetLifeExtensionAnalytics /></PageErrorBoundary>} />
              <Route path="/green-ammonia-export" element={<PageErrorBoundary pageName="/green-ammonia-export"><GreenAmmoniaExportAnalytics /></PageErrorBoundary>} />
              <Route path="/electricity-export-cable" element={<PageErrorBoundary pageName="/electricity-export-cable"><ElectricityExportCableAnalytics /></PageErrorBoundary>} />
              <Route path="/industrial-decarbonisation" element={<PageErrorBoundary pageName="/industrial-decarbonisation"><IndustrialDecarbonisationAnalytics /></PageErrorBoundary>} />
              <Route path="/community-energy-storage" element={<PageErrorBoundary pageName="/community-energy-storage"><CommunityEnergyStorageAnalytics /></PageErrorBoundary>} />
              <Route path="/nem-generation-mix" element={<PageErrorBoundary pageName="/nem-generation-mix"><NEMGenerationMixAnalytics /></PageErrorBoundary>} />
              <Route path="/consumer-energy-affordability" element={<PageErrorBoundary pageName="/consumer-energy-affordability"><ConsumerEnergyAffordabilityAnalytics /></PageErrorBoundary>} />
              <Route path="/electricity-price-risk" element={<PageErrorBoundary pageName="/electricity-price-risk"><ElectricityPriceRiskAnalytics /></PageErrorBoundary>} />
              <Route path="/ev-fleet-depot" element={<PageErrorBoundary pageName="/ev-fleet-depot"><EVFleetDepotAnalytics /></PageErrorBoundary>} />
              <Route path="/wind-farm-wake" element={<PageErrorBoundary pageName="/wind-farm-wake"><WindFarmWakeAnalytics /></PageErrorBoundary>} />
              <Route path="/market-bidding-strategy" element={<PageErrorBoundary pageName="/market-bidding-strategy"><MarketBiddingStrategyAnalytics /></PageErrorBoundary>} />
              <Route path="/solar-pv-soiling" element={<PageErrorBoundary pageName="/solar-pv-soiling"><SolarPVSoilingAnalytics /></PageErrorBoundary>} />
              <Route path="/ot-ics-cyber-security" element={<PageErrorBoundary pageName="/ot-ics-cyber-security"><OTICSCyberSecurityAnalytics /></PageErrorBoundary>} />
              <Route path="/stpasa-adequacy" element={<PageErrorBoundary pageName="/stpasa-adequacy"><STPASAAdequacyAnalytics /></PageErrorBoundary>} />
              <Route path="/generator-performance-standards" element={<PageErrorBoundary pageName="/generator-performance-standards"><GeneratorPerformanceStandardsAnalytics /></PageErrorBoundary>} />
              <Route path="/biomass-bioenergy" element={<PageErrorBoundary pageName="/biomass-bioenergy"><BiomassBioenergyAnalytics /></PageErrorBoundary>} />
              <Route path="/electricity-frequency-performance" element={<PageErrorBoundary pageName="/electricity-frequency-performance"><ElectricityFrequencyPerformanceAnalytics /></PageErrorBoundary>} />
              <Route path="/lgc-market" element={<PageErrorBoundary pageName="/lgc-market"><LGCMarketAnalytics /></PageErrorBoundary>} />
              <Route path="/wave-tidal-ocean" element={<PageErrorBoundary pageName="/wave-tidal-ocean"><WaveTidalOceanAnalytics /></PageErrorBoundary>} />
              <Route path="/reactive-power-voltage" element={<PageErrorBoundary pageName="/reactive-power-voltage"><ReactivePowerVoltageAnalytics /></PageErrorBoundary>} />
              <Route path="/battery-revenue-stack" element={<PageErrorBoundary pageName="/battery-revenue-stack"><BatteryRevenueStackAnalytics /></PageErrorBoundary>} />
              <Route path="/digital-energy-twin" element={<PageErrorBoundary pageName="/digital-energy-twin"><DigitalEnergyTwinAnalytics /></PageErrorBoundary>} />
              <Route path="/network-protection-system" element={<PageErrorBoundary pageName="/network-protection-system"><NetworkProtectionSystemAnalytics /></PageErrorBoundary>} />
              <Route path="/pumped-hydro-dispatch" element={<PageErrorBoundary pageName="/pumped-hydro-dispatch"><PumpedHydroDispatchAnalytics /></PageErrorBoundary>} />
              <Route path="/retail-market-design" element={<PageErrorBoundary pageName="/retail-market-design"><RetailMarketDesignAnalytics /></PageErrorBoundary>} />
              <Route path="/spot-market-depth-x" element={<PageErrorBoundary pageName="/spot-market-depth-x"><SpotMarketDepthXAnalytics /></PageErrorBoundary>} />
              <Route path="/solar-farm-operations" element={<PageErrorBoundary pageName="/solar-farm-operations"><SolarFarmOperationsAnalytics /></PageErrorBoundary>} />
              <Route path="/distribution-network-planning" element={<PageErrorBoundary pageName="/distribution-network-planning"><DistributionNetworkPlanningAnalytics /></PageErrorBoundary>} />
              <Route path="/grid-flexibility-services" element={<PageErrorBoundary pageName="/grid-flexibility-services"><GridFlexibilityServicesAnalytics /></PageErrorBoundary>} />
              <Route path="/hydrogen-refuelling-station" element={<PageErrorBoundary pageName="/hydrogen-refuelling-station"><HydrogenRefuellingStationAnalytics /></PageErrorBoundary>} />
              <Route path="/offshore-wind-finance" element={<PageErrorBoundary pageName="/offshore-wind-finance"><OffshoreWindFinanceAnalytics /></PageErrorBoundary>} />
              <Route path="/carbon-offset-project" element={<PageErrorBoundary pageName="/carbon-offset-project"><CarbonOffsetProjectAnalytics /></PageErrorBoundary>} />
              <Route path="/power-grid-climate-resilience" element={<PageErrorBoundary pageName="/power-grid-climate-resilience"><PowerGridClimateResilienceAnalytics /></PageErrorBoundary>} />
              <Route path="/energy-storage-tech-comparison" element={<PageErrorBoundary pageName="/energy-storage-tech-comparison"><EnergyStorageTechComparisonAnalytics /></PageErrorBoundary>} />
              <Route path="/power-to-x-economics" element={<PageErrorBoundary pageName="/power-to-x-economics"><PowerToXEconomicsAnalytics /></PageErrorBoundary>} />
              <Route path="/electricity-market-microstructure" element={<PageErrorBoundary pageName="/electricity-market-microstructure"><ElectricityMarketMicrostructureAnalytics /></PageErrorBoundary>} />
              <Route path="/grid-decarbonisation-pathway" element={<PageErrorBoundary pageName="/grid-decarbonisation-pathway"><GridDecarbonisationPathwayAnalytics /></PageErrorBoundary>} />
              <Route path="/rooftop-solar-network-impact" element={<PageErrorBoundary pageName="/rooftop-solar-network-impact"><RooftopSolarNetworkImpactAnalytics /></PageErrorBoundary>} />
              <Route path="/electricity-network-tariff-reform" element={<PageErrorBoundary pageName="/electricity-network-tariff-reform"><ElectricityNetworkTariffReformAnalytics /></PageErrorBoundary>} />
              <Route path="/long-duration-energy-storage" element={<PageErrorBoundary pageName="/long-duration-energy-storage"><LongDurationEnergyStorageAnalytics /></PageErrorBoundary>} />
              <Route path="/hydrogen-pipeline-infrastructure" element={<PageErrorBoundary pageName="/hydrogen-pipeline-infrastructure"><HydrogenPipelineInfrastructureAnalytics /></PageErrorBoundary>} />
              <Route path="/carbon-capture-storage-project" element={<PageErrorBoundary pageName="/carbon-capture-storage-project"><CarbonCaptureStorageProjectAnalytics /></PageErrorBoundary>} />
              <Route path="/energy-poverty-vulnerable-consumer" element={<PageErrorBoundary pageName="/energy-poverty-vulnerable-consumer"><EnergyPovertyVulnerableConsumerAnalytics /></PageErrorBoundary>} />
              <Route path="/nuclear-small-modular-reactor" element={<PageErrorBoundary pageName="/nuclear-small-modular-reactor"><NuclearSmallModularReactorAnalytics /></PageErrorBoundary>} />
              <Route path="/electricity-market-transparency" element={<PageErrorBoundary pageName="/electricity-market-transparency"><ElectricityMarketTransparencyAnalytics /></PageErrorBoundary>} />
              <Route path="/geothermal-energy-development" element={<PageErrorBoundary pageName="/geothermal-energy-development"><GeothermalEnergyDevelopmentAnalytics /></PageErrorBoundary>} />
              <Route path="/solar-thermal-power-plant" element={<PageErrorBoundary pageName="/solar-thermal-power-plant"><SolarThermalPowerPlantAnalytics /></PageErrorBoundary>} />
              <Route path="/energy-trading-algorithmic-strategy" element={<PageErrorBoundary pageName="/energy-trading-algorithmic-strategy"><EnergyTradingAlgorithmicStrategyAnalytics /></PageErrorBoundary>} />
              <Route path="/ev-grid-integration-v2g" element={<PageErrorBoundary pageName="/ev-grid-integration-v2g"><EVGridIntegrationV2GAnalytics /></PageErrorBoundary>} />
              <Route path="/biomethane-gas-grid-injection" element={<PageErrorBoundary pageName="/biomethane-gas-grid-injection"><BiomethaneGasGridInjectionAnalytics /></PageErrorBoundary>} />
              <Route path="/electricity-market-forecasting-accuracy" element={<PageErrorBoundary pageName="/electricity-market-forecasting-accuracy"><ElectricityMarketForecastingAccuracyAnalytics /></PageErrorBoundary>} />
              <Route path="/national-energy-transition-investment" element={<PageErrorBoundary pageName="/national-energy-transition-investment"><NationalEnergyTransitionInvestmentAnalytics /></PageErrorBoundary>} />
              <Route path="/electricity-spot-price-seasonality" element={<PageErrorBoundary pageName="/electricity-spot-price-seasonality"><ElectricitySpotPriceSeasonalityAnalytics /></PageErrorBoundary>} />
              <Route path="/grid-congestion-constraint" element={<PageErrorBoundary pageName="/grid-congestion-constraint"><GridCongestionConstraintAnalytics /></PageErrorBoundary>} />
              <Route path="/electricity-market-competition-concentration" element={<PageErrorBoundary pageName="/electricity-market-competition-concentration"><ElectricityMarketCompetitionConcentrationAnalytics /></PageErrorBoundary>} />
              <Route path="/renewable-energy-zone-development" element={<PageErrorBoundary pageName="/renewable-energy-zone-development"><RenewableEnergyZoneDevelopmentAnalytics /></PageErrorBoundary>} />
              <Route path="/battery-storage-degradation-lifetime" element={<PageErrorBoundary pageName="/battery-storage-degradation-lifetime"><BatteryStorageDegradationLifetimeAnalytics /></PageErrorBoundary>} />
              <Route path="/electricity-consumer-switching-churn" element={<PageErrorBoundary pageName="/electricity-consumer-switching-churn"><ElectricityConsumerSwitchingChurnAnalytics /></PageErrorBoundary>} />
              <Route path="/nem-inertia-synchronous-condenser" element={<PageErrorBoundary pageName="/nem-inertia-synchronous-condenser"><NEMInertiaSynchronousCondenserAnalytics /></PageErrorBoundary>} />
              <Route path="/offshore-wind-leasing-site" element={<PageErrorBoundary pageName="/offshore-wind-leasing-site"><OffshoreWindLeasingSiteAnalytics /></PageErrorBoundary>} />
              <Route path="/electricity-market-regulatory-appeals" element={<PageErrorBoundary pageName="/electricity-market-regulatory-appeals"><ElectricityMarketRegulatoryAppealsAnalytics /></PageErrorBoundary>} />
              <Route path="/distributed-energy-resource-management" element={<PageErrorBoundary pageName="/distributed-energy-resource-management"><DistributedEnergyResourceManagementAnalytics /></PageErrorBoundary>} />
              <Route path="/market-price-formation-review" element={<PageErrorBoundary pageName="/market-price-formation-review"><MarketPriceFormationReviewAnalytics /></PageErrorBoundary>} />
              <Route path="/residential-solar-self-consumption" element={<PageErrorBoundary pageName="/residential-solar-self-consumption"><ResidentialSolarSelfConsumptionAnalytics /></PageErrorBoundary>} />
              <Route path="/energy-infrastructure-cyber-threat" element={<PageErrorBoundary pageName="/energy-infrastructure-cyber-threat"><EnergyInfrastructureCyberThreatAnalytics /></PageErrorBoundary>} />
              <Route path="/wholesale-gas-market" element={<PageErrorBoundary pageName="/wholesale-gas-market"><WholesaleGasMarketAnalytics /></PageErrorBoundary>} />
              <Route path="/electricity-demand-forecasting-ml" element={<PageErrorBoundary pageName="/electricity-demand-forecasting-ml"><ElectricityDemandForecastingMLAnalytics /></PageErrorBoundary>} />
              <Route path="/energy-storage-merchant-revenue" element={<PageErrorBoundary pageName="/energy-storage-merchant-revenue"><EnergyStorageMerchantRevenueAnalytics /></PageErrorBoundary>} />
              <Route path="/industrial-electrification-x" element={<PageErrorBoundary pageName="/industrial-electrification-x"><IndustrialElectrificationXAnalytics /></PageErrorBoundary>} />
              <Route path="/transmission-access-reform" element={<PageErrorBoundary pageName="/transmission-access-reform"><TransmissionAccessReformAnalytics /></PageErrorBoundary>} />
              <Route path="/hydrogen-export-terminal" element={<PageErrorBoundary pageName="/hydrogen-export-terminal"><HydrogenExportTerminalAnalytics /></PageErrorBoundary>} />
              <Route path="/grid-edge-technology-x" element={<PageErrorBoundary pageName="/grid-edge-technology-x"><GridEdgeTechnologyXAnalytics /></PageErrorBoundary>} />
              <Route path="/energy-retailer-margin" element={<PageErrorBoundary pageName="/energy-retailer-margin"><EnergyRetailerMarginAnalytics /></PageErrorBoundary>} />
              <Route path="/ev-fleet-charging" element={<PageErrorBoundary pageName="/ev-fleet-charging"><EvFleetChargingAnalytics /></PageErrorBoundary>} />
              <Route path="/carbon-border-adjustment" element={<PageErrorBoundary pageName="/carbon-border-adjustment"><CarbonBorderAdjustmentAnalytics /></PageErrorBoundary>} />
              <Route path="/power-purchase-agreement-market" element={<PageErrorBoundary pageName="/power-purchase-agreement-market"><PowerPurchaseAgreementMarketAnalytics /></PageErrorBoundary>} />
              <Route path="/distributed-solar-forecasting" element={<PageErrorBoundary pageName="/distributed-solar-forecasting"><DistributedSolarForecastingAnalytics /></PageErrorBoundary>} />
              <Route path="/electricity-market-liquidity" element={<PageErrorBoundary pageName="/electricity-market-liquidity"><ElectricityMarketLiquidityAnalytics /></PageErrorBoundary>} />
              <Route path="/energy-transition-finance-x" element={<PageErrorBoundary pageName="/energy-transition-finance-x"><EnergyTransitionFinanceXAnalytics /></PageErrorBoundary>} />
              <Route path="/nem-frequency-control" element={<PageErrorBoundary pageName="/nem-frequency-control"><NemFrequencyControlAnalytics /></PageErrorBoundary>} />
              <Route path="/battery-second-life" element={<PageErrorBoundary pageName="/battery-second-life"><BatterySecondLifeAnalytics /></PageErrorBoundary>} />
              <Route path="/utility-solar-farm-operations" element={<PageErrorBoundary pageName="/utility-solar-farm-operations"><UtilitySolarFarmOperationsAnalytics /></PageErrorBoundary>} />
              <Route path="/wind-farm-wake-effect" element={<PageErrorBoundary pageName="/wind-farm-wake-effect"><WindFarmWakeEffectAnalytics /></PageErrorBoundary>} />
              <Route path="/energy-poverty-hardship" element={<PageErrorBoundary pageName="/energy-poverty-hardship"><EnergyPovertyHardshipAnalytics /></PageErrorBoundary>} />
              <Route path="/electricity-network-capital-investment" element={<PageErrorBoundary pageName="/electricity-network-capital-investment"><ElectricityNetworkCapitalInvestmentAnalytics /></PageErrorBoundary>} />
              <Route path="/gas-to-power-transition" element={<PageErrorBoundary pageName="/gas-to-power-transition"><GasToPowerTransitionAnalytics /></PageErrorBoundary>} />
              <Route path="/carbon-offset-market" element={<PageErrorBoundary pageName="/carbon-offset-market"><CarbonOffsetMarketAnalytics /></PageErrorBoundary>} />
              <Route path="/power-system-stability-x" element={<PageErrorBoundary pageName="/power-system-stability-x"><PowerSystemStabilityXAnalytics /></PageErrorBoundary>} />
              <Route path="/aemo-market-operations" element={<PageErrorBoundary pageName="/aemo-market-operations"><AemoMarketOperationsAnalytics /></PageErrorBoundary>} />
              <Route path="/renewable-energy-certificate" element={<PageErrorBoundary pageName="/renewable-energy-certificate"><RenewableEnergyCertificateAnalytics /></PageErrorBoundary>} />
              <Route path="/energy-storage-dispatch-optimisation" element={<PageErrorBoundary pageName="/energy-storage-dispatch-optimisation"><EnergyStorageDispatchOptimisationAnalytics /></PageErrorBoundary>} />
              <Route path="/offshore-wind-project-finance" element={<PageErrorBoundary pageName="/offshore-wind-project-finance"><OffshoreWindProjectFinanceAnalytics /></PageErrorBoundary>} />
              <Route path="/national-energy-market-reform" element={<PageErrorBoundary pageName="/national-energy-market-reform"><NationalEnergyMarketReformAnalytics /></PageErrorBoundary>} />
              <Route path="/electricity-market-price-formation" element={<PageErrorBoundary pageName="/electricity-market-price-formation"><ElectricityMarketPriceFormationAnalytics /></PageErrorBoundary>} />
              <Route path="/rez-auction-cis" element={<PageErrorBoundary pageName="/rez-auction-cis"><RezAuctionCisAnalytics /></PageErrorBoundary>} />
              <Route path="/grid-modernisation-digital-twin" element={<PageErrorBoundary pageName="/grid-modernisation-digital-twin"><GridModernisationDigitalTwinAnalytics /></PageErrorBoundary>} />
              <Route path="/energy-market-credit-risk" element={<PageErrorBoundary pageName="/energy-market-credit-risk"><EnergyMarketCreditRiskAnalytics /></PageErrorBoundary>} />
              <Route path="/electricity-consumer-behaviour" element={<PageErrorBoundary pageName="/electricity-consumer-behaviour"><ElectricityConsumerBehaviourAnalytics /></PageErrorBoundary>} />
              <Route path="/thermal-coal-power-transition" element={<PageErrorBoundary pageName="/thermal-coal-power-transition"><ThermalCoalPowerTransitionAnalytics /></PageErrorBoundary>} />
              <Route path="/demand-response-aggregator" element={<PageErrorBoundary pageName="/demand-response-aggregator"><DemandResponseAggregatorAnalytics /></PageErrorBoundary>} />
              <Route path="/energy-commodity-trading" element={<PageErrorBoundary pageName="/energy-commodity-trading"><EnergyCommodityTradingAnalytics /></PageErrorBoundary>} />
              <Route path="/network-tariff-design-reform" element={<PageErrorBoundary pageName="/network-tariff-design-reform"><NetworkTariffDesignReformAnalytics /></PageErrorBoundary>} />
              <Route path="/hydrogen-valley-cluster" element={<PageErrorBoundary pageName="/hydrogen-valley-cluster"><HydrogenValleyClusterAnalytics /></PageErrorBoundary>} />
              <Route path="/nem-congestion-rent" element={<PageErrorBoundary pageName="/nem-congestion-rent"><NemCongestionRentAnalytics /></PageErrorBoundary>} />
              <Route path="/electricity-retailer-churn" element={<PageErrorBoundary pageName="/electricity-retailer-churn"><ElectricityRetailerChurnAnalytics /></PageErrorBoundary>} />
              <Route path="/energy-asset-maintenance" element={<PageErrorBoundary pageName="/energy-asset-maintenance"><EnergyAssetMaintenanceAnalytics /></PageErrorBoundary>} />
              <Route path="/coal-seam-gas" element={<PageErrorBoundary pageName="/coal-seam-gas"><CoalSeamGasAnalytics /></PageErrorBoundary>} />
              <Route path="/ev-battery-technology" element={<PageErrorBoundary pageName="/ev-battery-technology"><EvBatteryTechnologyAnalytics /></PageErrorBoundary>} />
              <Route path="/nem-demand-forecasting-accuracy" element={<PageErrorBoundary pageName="/nem-demand-forecasting-accuracy"><NemDemandForecastingAccuracyAnalytics /></PageErrorBoundary>} />
              <Route path="/power-system-inertia" element={<PageErrorBoundary pageName="/power-system-inertia"><PowerSystemInertiaAnalytics /></PageErrorBoundary>} />
              <Route path="/electricity-network-investment-deferral" element={<PageErrorBoundary pageName="/electricity-network-investment-deferral"><ElectricityNetworkInvestmentDeferralAnalytics /></PageErrorBoundary>} />
              <Route path="/rez-capacity-factor" element={<PageErrorBoundary pageName="/rez-capacity-factor"><RezCapacityFactorAnalytics /></PageErrorBoundary>} />
              <Route path="/energy-retailer-hedging" element={<PageErrorBoundary pageName="/energy-retailer-hedging"><EnergyRetailerHedgingAnalytics /></PageErrorBoundary>} />
              <Route path="/gas-power-plant-flexibility" element={<PageErrorBoundary pageName="/gas-power-plant-flexibility"><GasPowerPlantFlexibilityAnalytics /></PageErrorBoundary>} />
              <Route path="/solar-irradiance-resource" element={<PageErrorBoundary pageName="/solar-irradiance-resource"><SolarIrradianceResourceAnalytics /></PageErrorBoundary>} />
              <Route path="/electricity-price-cap-intervention" element={<PageErrorBoundary pageName="/electricity-price-cap-intervention"><ElectricityPriceCapInterventionAnalytics /></PageErrorBoundary>} />
              <Route path="/biogas-landfill" element={<PageErrorBoundary pageName="/biogas-landfill"><BiogasLandfillAnalytics /></PageErrorBoundary>} />
              <Route path="/wind-resource-variability" element={<PageErrorBoundary pageName="/wind-resource-variability"><WindResourceVariabilityAnalytics /></PageErrorBoundary>} />
              <Route path="/energy-storage-duration" element={<PageErrorBoundary pageName="/energy-storage-duration"><EnergyStorageDurationAnalytics /></PageErrorBoundary>} />
              <Route path="/nem-settlement-residue-auction" element={<PageErrorBoundary pageName="/nem-settlement-residue-auction"><NemSettlementResidueAuctionAnalytics /></PageErrorBoundary>} />
              <Route path="/hydrogen-electrolysis-cost" element={<PageErrorBoundary pageName="/hydrogen-electrolysis-cost"><HydrogenElectrolysisCostAnalytics /></PageErrorBoundary>} />
              <Route path="/electricity-demand-elasticity" element={<PageErrorBoundary pageName="/electricity-demand-elasticity"><ElectricityDemandElasticityAnalytics /></PageErrorBoundary>} />
              <Route path="/nuclear-energy-feasibility" element={<PageErrorBoundary pageName="/nuclear-energy-feasibility"><NuclearEnergyFeasibilityAnalytics /></PageErrorBoundary>} />
              <Route path="/transmission-congestion-revenue" element={<PageErrorBoundary pageName="/transmission-congestion-revenue"><TransmissionCongestionRevenueAnalytics /></PageErrorBoundary>} />
              <Route path="/electricity-market-design-reform" element={<PageErrorBoundary pageName="/electricity-market-design-reform"><ElectricityMarketDesignReformAnalytics /></PageErrorBoundary>} />
              <Route path="/carbon-capture-utilisation" element={<PageErrorBoundary pageName="/carbon-capture-utilisation"><CarbonCaptureUtilisationAnalytics /></PageErrorBoundary>} />
              <Route path="/grid-scale-battery-degradation" element={<PageErrorBoundary pageName="/grid-scale-battery-degradation"><GridScaleBatteryDegradationAnalytics /></PageErrorBoundary>} />
              <Route path="/electricity-export" element={<PageErrorBoundary pageName="/electricity-export"><AustraliaElectricityExportAnalytics /></PageErrorBoundary>} />
              <Route path="/dsm-programs" element={<PageErrorBoundary pageName="/dsm-programs"><DemandSideManagementProgramAnalytics /></PageErrorBoundary>} />
              <Route path="/grid-topology" element={<PageErrorBoundary pageName="/grid-topology"><PowerGridTopologyAnalytics /></PageErrorBoundary>} />
              <Route path="/rooftop-solar-fit" element={<PageErrorBoundary pageName="/rooftop-solar-fit"><RooftopSolarFeedInTariffAnalytics /></PageErrorBoundary>} />
              <Route path="/lng-export" element={<PageErrorBoundary pageName="/lng-export"><LngExportAnalytics /></PageErrorBoundary>} />
              <Route path="/settings"          element={<Settings />}            />
              <Route path="/community-microgrids" element={<PageErrorBoundary pageName="/community-microgrids"><EnergyCommunityMicrogridAnalytics /></PageErrorBoundary>} />
              <Route path="/ewml-dashboard" element={<PageErrorBoundary pageName="/ewml-dashboard"><ElectricityWholesaleMarketLiquidityAnalytics /></PageErrorBoundary>} />
              <Route path="/marine-energy" element={<PageErrorBoundary pageName="/marine-energy"><TidalWaveMarineEnergyAnalytics /></PageErrorBoundary>} />
              <Route path="/geothermal-energy" element={<PageErrorBoundary pageName="/geothermal-energy"><GeothermalEnergyAnalytics /></PageErrorBoundary>} />
              <Route path="/storage-arbitrage" element={<PageErrorBoundary pageName="/storage-arbitrage"><EnergyStorageArbitrageAnalytics /></PageErrorBoundary>} />
              <Route path="/carbon-border-adjustment-x" element={<PageErrorBoundary pageName="/carbon-border-adjustment-x"><CarbonBorderAdjustmentXAnalytics /></PageErrorBoundary>} />
              <Route path="/fcas-procurement" element={<PageErrorBoundary pageName="/fcas-procurement"><FcasProcurementAnalytics /></PageErrorBoundary>} />
              <Route path="/electricity-futures-options" element={<PageErrorBoundary pageName="/electricity-futures-options"><ElectricityFuturesOptionsAnalytics /></PageErrorBoundary>} />
              <Route path="/rec-lgc-stc" element={<PageErrorBoundary pageName="/rec-lgc-stc"><RenewableEnergyCertificateXAnalytics /></PageErrorBoundary>} />
              <Route path="/der-management-x" element={<PageErrorBoundary pageName="/der-management-x"><DistributedEnergyResourceManagementXAnalytics /></PageErrorBoundary>} />
              <Route path="/coal-mine-energy" element={<PageErrorBoundary pageName="/coal-mine-energy"><CoalMineEnergyAnalytics /></PageErrorBoundary>} />
              <Route path="/nem-5min-settlement" element={<PageErrorBoundary pageName="/nem-5min-settlement"><NemFiveMinuteSettlementAnalytics /></PageErrorBoundary>} />
              <Route path="/network-congestion-relief" element={<PageErrorBoundary pageName="/network-congestion-relief"><NetworkCongestionReliefAnalytics /></PageErrorBoundary>} />
              <Route path="/market-concentration-bidding" element={<PageErrorBoundary pageName="/market-concentration-bidding"><MarketConcentrationBiddingAnalytics /></PageErrorBoundary>} />
              <Route path="/industrial-energy-efficiency" element={<PageErrorBoundary pageName="/industrial-energy-efficiency"><IndustrialEnergyEfficiencyAnalytics /></PageErrorBoundary>} />
              <Route path="/retailer-financial-health" element={<PageErrorBoundary pageName="/retailer-financial-health"><RetailerFinancialHealthAnalytics /></PageErrorBoundary>} />
              <Route path="/solar-farm-performance" element={<PageErrorBoundary pageName="/solar-farm-performance"><SolarFarmPerformanceAnalytics /></PageErrorBoundary>} />
              <Route path="/gas-network-pipeline" element={<PageErrorBoundary pageName="/gas-network-pipeline"><GasNetworkPipelineAnalytics /></PageErrorBoundary>} />
              <Route path="/electricity-price-forecasting" element={<PageErrorBoundary pageName="/electricity-price-forecasting"><ElectricityPriceForecastingAnalytics /></PageErrorBoundary>} />
              <Route path="/network-asset-life-cycle" element={<PageErrorBoundary pageName="/network-asset-life-cycle"><NetworkAssetLifeCycleAnalytics /></PageErrorBoundary>} />
              <Route path="/wind-farm-wake-turbine" element={<PageErrorBoundary pageName="/wind-farm-wake-turbine"><WindFarmWakeTurbineAnalytics /></PageErrorBoundary>} />
              <Route path="/energy-poverty-hardship-x" element={<PageErrorBoundary pageName="/energy-poverty-hardship-x"><EnergyPovertyHardshipXAnalytics /></PageErrorBoundary>} />
              <Route path="/hydrogen-refuelling-transport" element={<PageErrorBoundary pageName="/hydrogen-refuelling-transport"><HydrogenRefuellingTransportAnalytics /></PageErrorBoundary>} />
              <Route path="/electricity-spot-price-events" element={<PageErrorBoundary pageName="/electricity-spot-price-events"><ElectricitySpotPriceEventAnalytics /></PageErrorBoundary>} />
              <Route path="/large-scale-renewable-auction" element={<PageErrorBoundary pageName="/large-scale-renewable-auction"><LargeScaleRenewableAuctionAnalytics /></PageErrorBoundary>} />
              <Route path="/nem-ancillary-services" element={<PageErrorBoundary pageName="/nem-ancillary-services"><NemAncillaryServicesAnalytics /></PageErrorBoundary>} />
              <Route path="/australian-carbon-credit" element={<PageErrorBoundary pageName="/australian-carbon-credit"><AustralianCarbonCreditAnalytics /></PageErrorBoundary>} />
              <Route path="/ev-grid-integration" element={<PageErrorBoundary pageName="/ev-grid-integration"><ElectricVehicleGridIntegrationAnalytics /></PageErrorBoundary>} />
              <Route path="/generator-capacity-adequacy" element={<PageErrorBoundary pageName="/generator-capacity-adequacy"><GeneratorCapacityAdequacyAnalytics /></PageErrorBoundary>} />
              <Route path="/smart-grid-cybersecurity" element={<PageErrorBoundary pageName="/smart-grid-cybersecurity"><SmartGridCybersecurityAnalytics /></PageErrorBoundary>} />
              <Route path="/pumped-hydro-reservoir" element={<PageErrorBoundary pageName="/pumped-hydro-reservoir"><PumpedHydroReservoirAnalytics /></PageErrorBoundary>} />
              <Route path="/etjj-dashboard" element={<PageErrorBoundary pageName="/etjj-dashboard"><EnergyTransitionJobsAnalytics /></PageErrorBoundary>} />
              <Route path="/nifr-dashboard" element={<PageErrorBoundary pageName="/nifr-dashboard"><InterconnectorFlowRightsAnalytics /></PageErrorBoundary>} />
              <Route path="/cefa-dashboard-x" element={<PageErrorBoundary pageName="/cefa-dashboard-x"><CleanEnergyFinanceXAnalytics /></PageErrorBoundary>} />
              <Route path="/bess-performance" element={<PageErrorBoundary pageName="/bess-performance"><BessPerformanceAnalytics /></PageErrorBoundary>} />
              <Route path="/elca-dashboard" element={<PageErrorBoundary pageName="/elca-dashboard"><LoadCurveAnalytics /></PageErrorBoundary>} />
              <Route path="/ngts-dashboard" element={<PageErrorBoundary pageName="/ngts-dashboard"><NaturalGasTradingAnalytics /></PageErrorBoundary>} />
              <Route path="/aeos-dashboard" element={<PageErrorBoundary pageName="/aeos-dashboard"><EnergyOptimisationAnalytics /></PageErrorBoundary>} />
              <Route path="/aemc-rule-change" element={<PageErrorBoundary pageName="/aemc-rule-change"><AemcRuleChangeAnalytics /></PageErrorBoundary>} />
              <Route path="/renx-dashboard" element={<PageErrorBoundary pageName="/renx-dashboard"><RenewableExportAnalytics /></PageErrorBoundary>} />
              <Route path="/cppa-dashboard-x" element={<PageErrorBoundary pageName="/cppa-dashboard-x"><CorporatePpaAnalytics /></PageErrorBoundary>} />
              <Route path="/nzem-dashboard" element={<PageErrorBoundary pageName="/nzem-dashboard"><NetZeroEmissionsAnalytics /></PageErrorBoundary>} />
              <Route path="/epsa-dashboard" element={<PageErrorBoundary pageName="/epsa-dashboard"><PriceSensitivityAnalytics /></PageErrorBoundary>} />
              <Route path="/grpt-dashboard" element={<PageErrorBoundary pageName="/grpt-dashboard"><GridReliabilityAnalytics /></PageErrorBoundary>} />
              <Route path="/mats-dashboard" element={<PageErrorBoundary pageName="/mats-dashboard"><MarketTradingStrategyAnalytics /></PageErrorBoundary>} />
              <Route path="/emga-dashboard" element={<PageErrorBoundary pageName="/emga-dashboard"><EmergingMarketsAnalytics /></PageErrorBoundary>} />
              <Route path="/epro-dashboard" element={<PageErrorBoundary pageName="/epro-dashboard"><PortfolioRiskOptimisationAnalytics /></PageErrorBoundary>} />
              <Route path="/aehm-dashboard" element={<PageErrorBoundary pageName="/aehm-dashboard"><EnergyHubMicrostructureAnalytics /></PageErrorBoundary>} />
              <Route path="/fmrp-dashboard" element={<PageErrorBoundary pageName="/fmrp-dashboard"><FrequencyReservePlanningAnalytics /></PageErrorBoundary>} />
              <Route path="/daro-dashboard" element={<PageErrorBoundary pageName="/daro-dashboard"><DistributedAssetOptimisationAnalytics /></PageErrorBoundary>} />
              <Route path="/ecsa-dashboard" element={<PageErrorBoundary pageName="/ecsa-dashboard"><ConsumerSegmentationAnalytics /></PageErrorBoundary>} />
              <Route path="/gena-dashboard" element={<PageErrorBoundary pageName="/gena-dashboard"><GenerationExpansionAnalytics /></PageErrorBoundary>} />
              <Route path="/nema-dashboard" element={<PageErrorBoundary pageName="/nema-dashboard"><MarketAnomalyDetectionAnalytics /></PageErrorBoundary>} />
              <Route path="/wcms-dashboard" element={<PageErrorBoundary pageName="/wcms-dashboard"><WindCapacityMarketAnalytics /></PageErrorBoundary>} />
              <Route path="/spar-dashboard" element={<PageErrorBoundary pageName="/spar-dashboard"><SolarParkRegistryAnalytics /></PageErrorBoundary>} />
              <Route path="/energy-storage-duration-x" element={<PageErrorBoundary pageName="/energy-storage-duration-x"><EnergyStorageDurationXAnalytics /></PageErrorBoundary>} />
              <Route path="/offshore-wind-development" element={<PageErrorBoundary pageName="/offshore-wind-development"><OffshoreWindDevelopmentAnalytics /></PageErrorBoundary>} />
              <Route path="/nem-participant-financial" element={<PageErrorBoundary pageName="/nem-participant-financial"><NemParticipantFinancialAnalytics /></PageErrorBoundary>} />
              <Route path="/green-tariff-hydrogen" element={<PageErrorBoundary pageName="/green-tariff-hydrogen"><GreenTariffHydrogenAnalytics /></PageErrorBoundary>} />
              <Route path="/nem-price-review" element={<PageErrorBoundary pageName="/nem-price-review"><NemPriceReviewAnalytics /></PageErrorBoundary>} />
              <Route path="/renewable-certificate-nem" element={<PageErrorBoundary pageName="/renewable-certificate-nem"><RenewableCertificateNemAnalytics /></PageErrorBoundary>} />
              <Route path="/demand-curve-price-anchor" element={<PageErrorBoundary pageName="/demand-curve-price-anchor"><DemandCurvePriceAnchorAnalytics /></PageErrorBoundary>} />
              <Route path="/market-evolution-policy" element={<PageErrorBoundary pageName="/market-evolution-policy"><MarketEvolutionPolicyAnalytics /></PageErrorBoundary>} />
              <Route path="/energy-grid-topology" element={<PageErrorBoundary pageName="/energy-grid-topology"><EnergyGridTopologyAnalytics /></PageErrorBoundary>} />
              <Route path="/carbon-voluntary-exchange" element={<PageErrorBoundary pageName="/carbon-voluntary-exchange"><CarbonVoluntaryExchangeAnalytics /></PageErrorBoundary>} />
              <Route path="/renewable-market-sensitivity" element={<PageErrorBoundary pageName="/renewable-market-sensitivity"><RenewableMarketSensitivityAnalytics /></PageErrorBoundary>} />
              <Route path="/natural-gas-pipeline" element={<PageErrorBoundary pageName="/natural-gas-pipeline"><NaturalGasPipelineAnalytics /></PageErrorBoundary>} />
              <Route path="/battery-chemistry-risk" element={<PageErrorBoundary pageName="/battery-chemistry-risk"><BatteryChemistryRiskAnalytics /></PageErrorBoundary>} />
              <Route path="/aemo-5min-settlement" element={<PageErrorBoundary pageName="/aemo-5min-settlement"><Aemo5MinSettlementAnalytics /></PageErrorBoundary>} />
              <Route path="/retail-competition" element={<PageErrorBoundary pageName="/retail-competition"><ElectricityRetailCompetitionAnalytics /></PageErrorBoundary>} />
              <Route path="/demand-response-aggregation" element={<PageErrorBoundary pageName="/demand-response-aggregation"><DemandResponseAggregationAnalytics /></PageErrorBoundary>} />
              <Route path="/grid-frequency" element={<PageErrorBoundary pageName="/grid-frequency"><GridFrequencyResponseAnalytics /></PageErrorBoundary>} />
              <Route path="/hydrogen-economy-outlook" element={<PageErrorBoundary pageName="/hydrogen-economy-outlook"><HydrogenEconomyOutlookAnalytics /></PageErrorBoundary>} />
              <Route path="/network-augmentation" element={<PageErrorBoundary pageName="/network-augmentation"><NetworkAugmentationDeferralAnalytics /></PageErrorBoundary>} />
              <Route path="/ev-fleet-grid" element={<PageErrorBoundary pageName="/ev-fleet-grid"><EvFleetGridIntegrationAnalytics /></PageErrorBoundary>} />
              <Route path="/grid-emissions-intensity" element={<PageErrorBoundary pageName="/grid-emissions-intensity"><GridEmissionsIntensityAnalytics /></PageErrorBoundary>} />
              <Route path="/microgrid-resilience" element={<PageErrorBoundary pageName="/microgrid-resilience"><MicrogridResilienceAnalytics /></PageErrorBoundary>} />
              <Route path="/power-quality" element={<PageErrorBoundary pageName="/power-quality"><PowerQualityMonitoringAnalytics /></PageErrorBoundary>} />
              <Route path="/vpp-operations" element={<PageErrorBoundary pageName="/vpp-operations"><VirtualPowerPlantOperationsAnalytics /></PageErrorBoundary>} />
              <Route path="/pumped-hydro" element={<PageErrorBoundary pageName="/pumped-hydro"><PumpedHydroStorageAnalytics /></PageErrorBoundary>} />
              <Route path="/smart-meter" element={<PageErrorBoundary pageName="/smart-meter"><SmartMeterDataAnalytics /></PageErrorBoundary>} />
              <Route path="/east-coast-gas" element={<PageErrorBoundary pageName="/east-coast-gas"><EastCoastGasMarketAnalytics /></PageErrorBoundary>} />
              <Route path="/isp-analytics" element={<PageErrorBoundary pageName="/isp-analytics"><IntegratedSystemPlanAnalytics /></PageErrorBoundary>} />
              <Route path="/community-battery" element={<PageErrorBoundary pageName="/community-battery"><CommunityBatteryAnalytics /></PageErrorBoundary>} />
              <Route path="/energy-cyber-security" element={<PageErrorBoundary pageName="/energy-cyber-security"><EnergySectorCyberSecurityAnalytics /></PageErrorBoundary>} />
              <Route path="/wholesale-market-reform" element={<PageErrorBoundary pageName="/wholesale-market-reform"><WholesaleMarketReformAnalytics /></PageErrorBoundary>} />
              <Route path="/nem-map" element={<PageErrorBoundary pageName="/nem-map"><NemInfrastructureMap /></PageErrorBoundary>} />
            </Routes>
          </main>
        </div>
      </div>
    </BrowserRouter>
  )
}
